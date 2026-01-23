#!/usr/bin/env python3
"""
Fetch a Netkeiba race entry page (出馬表) and odds, write a prediction input parquet,
then run the existing `netkeiba_predict.py` to produce model-based predictions.

Usage: RACE_ID=202509020411 python3 scripts/fetch_and_predict_from_netkeiba.py
"""
import os
import re
import sys
import subprocess
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# simple sire -> group mapping (substring match)
SIRE_GROUP_MAP = {
    'ディープインパクト': 'ディープ系',
    'ハーツクライ': 'ハーツ系',
    'ロードカナロア': 'ロード系',
    'キングカメハメハ': 'キングカメハメハ系',
    'キングカメハメハ': 'キング系',
    'ステイゴールド': 'ステイゴールド系',
    'エピファネイア': 'エピファネイア系',
    'キズナ': 'キズナ系',
}


def map_sire_to_group(name: str) -> str:
    if not name or pd.isna(name):
        return '不明'
    for k, v in SIRE_GROUP_MAP.items():
        if k in name:
            return v
    return 'その他'


def fetch_shutuba(race_id: str):
    url = f'https://race.netkeiba.com/race/shutuba.html?race_id={race_id}'
    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
    r.raise_for_status()
    if r.apparent_encoding:
        r.encoding = r.apparent_encoding
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = []
    # extract race-level metadata: date, name, distance/surface
    race_title = soup.title.string if soup.title else ''
    race_name = ''
    race_date = ''
    mdate = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日)', race_title)
    if mdate:
        race_date = mdate.group(1)
    # try to find distance/surface string like '芝2000m (右 B)'
    page_text = ' '.join(soup.stripped_strings)
    mds = re.search(r'(芝|ダ)\s*(\d{3,4})m', page_text)
    surface = mds.group(1) if mds else ''
    distance = int(mds.group(2)) if mds else None
    mdir = re.search(r'[(（]([^）)]+?右|左)[)）]', page_text)
    track_dir = None
    if '右' in page_text:
        track_dir = '右'
    elif '左' in page_text:
        track_dir = '左'

    # Try to get race class (G1/G2/G3/オープン)
    mclass = re.search(r'(G\d|Jpn1|オープン|OP|指定)', page_text)
    race_class = mclass.group(1) if mclass else ''

    # Try to parse rows with finer-grained fields
    for tr in soup.select('table tr'):
        text = ' '.join(tr.stripped_strings)
        if not text:
            continue
        hn_tag = tr.select_one('.HorseNameSpan') or tr.select_one('.HorseName')
        if hn_tag:
            horse_name = hn_tag.get_text(strip=True)
            # horse page link if available
            a = hn_tag.find_parent('a') or hn_tag.find('a')
            horse_url = None
            if a and a.get('href'):
                href = a.get('href')
                if href.startswith('/'):
                    horse_url = 'https://race.netkeiba.com' + href
                else:
                    horse_url = href
        else:
            # fallback: extract before sex marker
            m = re.search(r'([\u3000\u3040-\u30FF\u4E00-\u9FFF\w\-ー]+)\s*(?:牡|牝|セ)', text)
            horse_name = m.group(1).strip() if m else None
            horse_url = None
        if not horse_name:
            continue
        # age/sex
        m_as = re.search(r'(牡|牝|セ)\s*(\d{1,2})', text)
        age_sex = (m_as.group(1) + str(m_as.group(2))) if m_as else None
        # jockey/trainer/weight
        m_w = re.search(r'\s(\d{1,3}\.\d)\s', text)
        jin = float(m_w.group(1)) if m_w else None
        # body weight like 510(+4)
        m_bw = re.search(r'(\d{3})\s*\([+\-]?\d+\)', text)
        body_weight = int(m_bw.group(1)) if m_bw else None
        # odds
        m_od = re.search(r'(\d{1,3}\.\d)', text)
        odds = float(m_od.group(1)) if m_od else None

        # try to fetch sire (father) from horse page if link available (best-effort)
        father = None
        if horse_url:
            try:
                hr = requests.get(horse_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                hr.raise_for_status()
                if hr.apparent_encoding:
                    hr.encoding = hr.apparent_encoding
                htext = hr.text
                # try to find 父 or 父名 nearby
                m_f = re.search(r'父(?:名|)[:：\s]*([\u3000\u3040-\u30FF\u4E00-\u9FFF\w\-ー]+)', htext)
                if m_f:
                    father = m_f.group(1).strip()
                else:
                    # fallback: look for '血統' block and take first link text
                    hsoup = BeautifulSoup(htext, 'html.parser')
                    kb = hsoup.find(text=re.compile('血統'))
                    if kb:
                        # search parent for links
                        p = kb.parent
                        if p:
                            a = p.find('a')
                            if a:
                                father = a.get_text(strip=True)
            except Exception:
                father = None

        rows.append({
            'horse_name': horse_name,
            'odds': odds,
            'age_sex': age_sex,
            'weight': jin,
            'body_weight': body_weight,
            'father_group': map_sire_to_group(father),
            'horse_url': horse_url,
            'race_url': url,
            'race_date': race_date,
            'race_class': race_class,
            'surface': surface,
            'distance': distance,
            'track_dir': track_dir,
        })

    if not rows:
        raise RuntimeError(f'No horses parsed from {url}')
    # dedupe preserving order
    seen = set(); uniq = []
    for r in rows:
        if r['horse_name'] in seen:
            continue
        seen.add(r['horse_name'])
        uniq.append(r)
    df = pd.DataFrame(uniq)
    # drop obvious header rows like '馬名'
    df = df[~df['horse_name'].astype(str).str.match(r"^\s*馬名\s*$")].reset_index(drop=True)
    return df


def fetch_result_odds_if_missing(df: pd.DataFrame, race_id: str):
    # try to get odds from result page if some odds missing
    if df['odds'].notna().all():
        return df
    url = f'https://race.netkeiba.com/race/result.html?race_id={race_id}'
    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
    r.raise_for_status()
    if r.apparent_encoding:
        r.encoding = r.apparent_encoding
    text = r.text
    odds_map = {}
    for name in df['horse_name']:
        if pd.isna(name):
            continue
        m = re.search(re.escape(name) + r'.{0,120}?(\d{1,3}\.\d)', text, flags=re.S)
        if m:
            odds_map[name] = float(m.group(1))
    if odds_map:
        df['odds'] = df['horse_name'].map(odds_map).astype(float)
    return df


def write_input_parquet(df: pd.DataFrame, race_id: str):
    out_dir = Path('outputs')
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'prediction_inputs_{race_id}.parquet'
    df.to_parquet(path, index=False)
    return str(path)


def fetch_horse_history(horse_url: str, max_r=10):
    """Fetch recent finishing positions from a horse page (best-effort).
    Returns a list of ints (positions) or None."""
    if not horse_url:
        return None
    try:
        r = requests.get(horse_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        r.raise_for_status()
        if r.apparent_encoding:
            r.encoding = r.apparent_encoding
        text = r.text
        # find recent results table: look for patterns like '1着', '2着', etc.
        ranks = []
        # search for occurrences of '着' with a preceding number
        for m in re.finditer(r'(\d{1,2})着', text):
            ranks.append(int(m.group(1)))
            if len(ranks) >= max_r:
                break
        if ranks:
            return ranks
        # fallback: try to find '着順' column in tables
        soup = BeautifulSoup(text, 'html.parser')
        # look for result tables with class hint
        for table in soup.select('table'):
            ttxt = ' '.join(table.stripped_strings)
            ms = re.findall(r'(\d{1,2})着', ttxt)
            if ms:
                return [int(x) for x in ms[:max_r]]
    except Exception:
        return None
    return None


def run_predict(input_path: str, race_id: str):
    out_path = f'outputs/netkeiba_predictions_{race_id}.parquet'
    # compute absolute path to the shipped predictor script
    script = Path(os.getcwd()) / 'WeeklySignalScanner-main' / 'scripts' / 'netkeiba_predict.py'
    if not script.exists():
        raise FileNotFoundError(f'netkeiba_predict.py not found at {script}')
    env = os.environ.copy()
    env['NETKEIBA_IN'] = input_path
    env['NETKEIBA_OUT'] = out_path
    cmd = [sys.executable, str(script)]
    subprocess.check_call(cmd, env=env)
    return out_path


def main():
    race_id = os.environ.get('RACE_ID') or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not race_id:
        print('Specify RACE_ID via env or arg')
        sys.exit(2)
    print('Fetching shutuba for', race_id)
    df = fetch_shutuba(race_id)
    df = fetch_result_odds_if_missing(df, race_id)
    # ensure recent_ranks exists: fetch per-horse history if missing
    if 'recent_ranks' not in df.columns:
        df['recent_ranks'] = None
    for i, row in df.iterrows():
        if row.get('recent_ranks') is None or (isinstance(row.get('recent_ranks'), float) and pd.isna(row.get('recent_ranks'))):
            hurl = row.get('horse_url') if 'horse_url' in row else None
            if hurl:
                hr = fetch_horse_history(hurl, max_r=10)
                if hr:
                    df.at[i, 'recent_ranks'] = str(hr)
    # fill missing odds with NaN -> downstream featurize will handle
    input_path = write_input_parquet(df, race_id)
    print('Wrote input parquet:', input_path)
    try:
        out = run_predict(input_path, race_id)
        print('Predictions written to', out)
    except subprocess.CalledProcessError as e:
        print('Prediction script failed:', e)
        sys.exit(1)


if __name__ == '__main__':
    main()
