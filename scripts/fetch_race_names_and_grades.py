#!/usr/bin/env python3
"""Fetch race name and grade (G1/G2/etc) from netkeiba race pages and store into per-race parquet files.

Reads `outputs/predictions_by_race/summary_by_race.csv`, visits each `race_url` if the corresponding
per-race parquet lacks `race_name` or `race_class`, extracts title and grade, writes them into the
parquet (new columns `race_name`, `race_class`) and updates the summary CSV.

Be polite: small sleep between requests. Skips entries without a valid race_url.
"""
import time
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SUMMARY = ROOT / 'outputs' / 'predictions_by_race' / 'summary_by_race.csv'
BASE = ROOT / 'outputs' / 'predictions_by_race'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; NetKeibaFetcher/1.0; +https://example.com)'
}


def fetch_html(url: str, timeout: int = 20):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        # ensure correct encoding detection (netkeiba often uses EUC-JP / Shift-JIS)
        try:
            if r.apparent_encoding:
                r.encoding = r.apparent_encoding
        except Exception:
            pass
        return r.text
    except Exception:
        return None


def parse_title_and_grade(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    title = None
    # prefer h1/h2
    h = soup.find(['h1', 'h2'])
    if h:
        title = h.get_text(strip=True)
    if not title and soup.title:
        title = soup.title.string

    grade = None
    if title:
        for token in ['G1', 'G2', 'G3', 'Jpn1', 'Jpn2', 'Jpn3']:
            if token in title:
                grade = token
                break
        # also common Japanese words
        if not grade:
            if '重賞' in title or 'GI' in title or 'G I' in title:
                # normalize to G1 if ambiguous
                grade = 'G1'

    # fallback: search body for G1/G2 tokens
    if not grade:
        body = soup.get_text()
        m = re.search(r'\b(G[1-3]|GI|G II|G III|Jpn1|Jpn2|Jpn3)\b', body)
        if m:
            grade = m.group(1)

    return title, grade


def main():
    if not SUMMARY.exists():
        print('No summary file found at', SUMMARY)
        return
    df = pd.read_csv(SUMMARY)
    updated = []
    for _, row in df.iterrows():
        file_rel = row['file']
        race_url = row.get('race_url', '')
        if not isinstance(file_rel, str):
            continue
        p = ROOT / file_rel
        if not p.exists():
            p = Path.cwd() / file_rel
        if not p.exists():
            print('file missing, skipping', file_rel)
            continue

        # read per-race parquet and check if race_name or race_class present
        try:
            per = pd.read_parquet(p)
        except Exception as e:
            print('failed read', p, e)
            continue

        need = False
        if 'race_name' not in per.columns or per['race_name'].isna().all():
            need = True
        if 'race_class' not in per.columns or per['race_class'].isna().all():
            need = True

        if not need:
            continue

        if not isinstance(race_url, str) or not race_url.startswith('http'):
            print('no valid race_url for', file_rel)
            continue

        print('fetching', race_url)
        html = fetch_html(race_url)
        if not html:
            print('failed fetch', race_url)
            continue

        title, grade = parse_title_and_grade(html)
        print('=>', title, grade)

        # write back to parquet
        try:
            if 'race_name' not in per.columns:
                per['race_name'] = None
            if 'race_class' not in per.columns:
                per['race_class'] = None
            # assign
            per['race_name'] = title
            per['race_class'] = grade
            per.to_parquet(p, index=False)
            updated.append(file_rel)
        except Exception as e:
            print('failed write', p, e)

        time.sleep(0.5)

    if updated:
        print('updated', len(updated), 'files')
        # update summary entries
        for i, r in df.iterrows():
            if r['file'] in updated:
                p = ROOT / r['file']
                try:
                    per = pd.read_parquet(p)
                    df.at[i, 'race_url'] = r.get('race_url', '')
                    df.at[i, 'race_class'] = per['race_class'].iloc[0] if 'race_class' in per.columns else None
                except Exception:
                    continue
        df.to_csv(SUMMARY, index=False)
        print('summary updated')
    else:
        print('no files updated')


if __name__ == '__main__':
    main()
