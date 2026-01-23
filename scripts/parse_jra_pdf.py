#!/usr/bin/env python3
"""
parse_jra_pdf.py

Parse JRA race PDFs placed in a directory (`pdfs/` by default) and
produce a unified Parquet containing the requested fields.

This is a best-effort parser using `pdfplumber` and `tabula-py` as fallbacks.
It extracts header metadata and attempts to find the horse table.

Usage:
  python3 scripts/parse_jra_pdf.py --input-dir pdfs/ --out outputs/netkeiba_from_jra.parquet

"""
import argparse
import os
import re
import pandas as pd
import pdfplumber
from tabula import read_pdf as tabula_read


def extract_header_text(full_text):
    d = {}
    # date patterns: YYYY年M月D日 or YYYY-MM-DD
    m = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日)', full_text)
    if m:
        d['race_date'] = m.group(1)
    else:
        m2 = re.search(r'(\d{4}-\d{2}-\d{2})', full_text)
        if m2:
            d['race_date'] = m2.group(1)
    # track
    m = re.search(r'([\u4e00-\u9faf\u3040-\u30ff]+競馬場)', full_text)
    if m:
        d['track'] = m.group(1)
    # surface/distance
    m = re.search(r'(芝|ダート)\s*(\d{3,4})m', full_text)
    if m:
        d['surface'] = m.group(1)
        d['distance'] = int(m.group(2))
    # condition
    m = re.search(r'(良|稍重|重|不良)', full_text)
    if m:
        d['condition'] = m.group(1)
    return d


def try_tabula(pdf_path):
    try:
        tables = tabula_read(pdf_path, pages='all', multiple_tables=True)
        return tables
    except Exception:
        return None


def normalize_table(df):
    # heuristics to rename common Japanese headers to standard ones
    colmap = {}
    cols = list(df.columns)
    for c in cols:
        lc = str(c)
        if '馬' in lc and ('名' in lc or '馬名' in lc or lc.strip() in ('馬', '馬名')):
            colmap[c] = 'horse_name'
        if '枠' in lc and '番' not in lc:
            colmap[c] = 'frame_no'
        if '馬番' in lc or ('番' in lc and '馬番' not in lc):
            colmap[c] = 'horse_no'
        if '騎手' in lc:
            colmap[c] = 'jockey'
        if '斤量' in lc:
            colmap[c] = 'weight'
        if '馬体重' in lc:
            colmap[c] = 'body_weight'
        if '着' in lc and ('順' in lc or '着順' in lc):
            colmap[c] = 'rank'
        if '単勝' in lc or 'オッズ' in lc:
            colmap[c] = 'odds'
    return df.rename(columns=colmap)


def looks_like_horse_table(df):
    """Heuristic: detect horse table by presence of Japanese characters in cells.
    Returns True if many cells contain Kanji/Kana which likely indicate names.
    """
    if df is None or df.empty:
        return False
    sample = df.astype(str).values.flatten()
    # count cells with Japanese characters
    import re
    jp_re = re.compile(r'[\u3040-\u30ff\u4e00-\u9faf]')
    match_count = sum(1 for s in sample if jp_re.search(s))
    return match_count >= max(3, len(sample) // 20)


def parse_pdf_file(pdf_path):
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        full_text = '\n'.join(p.extract_text() or '' for p in pdf.pages)
        header = extract_header_text(full_text)

    # try tabula first
    tables = try_tabula(pdf_path)
    df_horse = None
    if tables:
        for t in tables:
            # guess: table that contains '馬' or '枠' is horse table
            try:
                if looks_like_horse_table(t):
                    df_horse = t
                    break
            except Exception:
                continue

    # fallback: try pdfplumber table extraction per page
    if df_horse is None:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                try:
                    tab = page.extract_table()
                    if not tab:
                        continue
                    df = pd.DataFrame(tab[1:], columns=tab[0])
                    try:
                        if looks_like_horse_table(df):
                            df_horse = df
                            break
                    except Exception:
                        continue
                except Exception:
                    continue

    if df_horse is None:
        # cannot parse table
        return pd.DataFrame()

    df_horse = normalize_table(df_horse)
    # If headers were garbage, try to detect horse_name column by content
    if 'horse_name' not in df_horse.columns:
        jp_re = re.compile(r'[\u3040-\u30ff\u4e00-\u9faf]')
        best_col = None
        best_count = 0
        for c in df_horse.columns:
            colvals = df_horse[c]
            cnt = sum(1 for v in colvals if jp_re.search(str(v)))
            if cnt > best_count:
                best_count = cnt
                best_col = c
        if best_col is not None and best_count >= max(2, len(df_horse) // 3):
            df_horse = df_horse.rename(columns={best_col: 'horse_name'})
    for _, r in df_horse.iterrows():
        row = dict(header)
        row.update({
            'horse_name': r.get('horse_name'),
            'frame_no': r.get('frame_no'),
            'horse_no': r.get('horse_no'),
            'jockey': r.get('jockey'),
            'weight': r.get('weight'),
            'body_weight': r.get('body_weight'),
            'rank': r.get('rank'),
            'odds': r.get('odds'),
        })
        rows.append(row)

    return pd.DataFrame(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input-dir', default='pdfs', help='Directory containing PDF files')
    p.add_argument('--out', default='outputs/netkeiba_from_jra.parquet', help='Output parquet path')
    args = p.parse_args()

    pdfs = []
    for fn in sorted(os.listdir(args.input_dir) if os.path.isdir(args.input_dir) else []):
        if fn.lower().endswith('.pdf'):
            pdfs.append(os.path.join(args.input_dir, fn))

    all_rows = []
    for ppath in pdfs:
        try:
            df = parse_pdf_file(ppath)
            if not df.empty:
                all_rows.append(df)
                print('Parsed', ppath, '->', len(df), 'rows')
            else:
                print('No table parsed for', ppath)
        except Exception as e:
            print('Error parsing', ppath, e)

    if all_rows:
        out_df = pd.concat(all_rows, ignore_index=True)
        out_dir = os.path.dirname(args.out)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        out_df.to_parquet(args.out, engine='pyarrow', index=False)
        print('Wrote', args.out, 'rows=', len(out_df))
    else:
        print('No data extracted')


if __name__ == '__main__':
    main()
