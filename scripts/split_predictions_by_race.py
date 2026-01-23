#!/usr/bin/env python3
"""Split master Netkeiba predictions parquet into per-race parquet files.

Writes to `outputs/predictions_by_race/{race_date}/{race_id}.parquet` and
creates `outputs/predictions_by_race/summary_by_race.csv` with counts.
Also prints top-N predictions per race to stdout.
"""
import os
import re
import sys
import pathlib
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
PREFERRED_PATHS = [
    ROOT / 'outputs' / 'netkeiba_predictions_with_probs.parquet',
    ROOT / 'WeeklySignalScanner-main' / 'outputs' / 'netkeiba_predictions_with_probs.parquet',
    ROOT / 'outputs' / 'netkeiba_predictions_from_jra.parquet',
]


def find_master_parquet():
    for p in PREFERRED_PATHS:
        if p.exists():
            return p
    # fallback: try outputs/*.parquet that look like predictions
    outdir = ROOT / 'outputs'
    if not outdir.exists():
        raise FileNotFoundError('outputs directory not found')
    for p in outdir.glob('*.parquet'):
        if 'predictions' in p.name or 'netkeiba' in p.name:
            return p
    raise FileNotFoundError('No suitable parquet found in outputs')


def sanitize_race_id(race_url: str) -> str:
    # try to extract trailing numeric id from Netkeiba race URL
    if not isinstance(race_url, str):
        return 'unknown'
    m = re.search(r'/(?:race|result)/([0-9]+)', race_url)
    if m:
        return m.group(1)
    # fallback: make filesystem-safe slug
    return re.sub(r'[^0-9A-Za-z_-]', '_', race_url)[:80]


def main():
    master = find_master_parquet()
    print('Using master parquet:', master)
    df = pd.read_parquet(master)
    if df.empty:
        print('Master parquet is empty; nothing to do.')
        return

    # Ensure race_date present
    if 'race_date' not in df.columns:
        # try to infer from race_url
        if 'race_url' in df.columns:
            df['race_date'] = df['race_url'].astype(str).str.extract(r'/(20[0-9]{2}[0-1][0-9][0-3][0-9])')[0]
        else:
            df['race_date'] = 'unknown_date'

    out_base = ROOT / 'outputs' / 'predictions_by_race'
    out_base.mkdir(parents=True, exist_ok=True)

    groups = df.groupby(['race_date', 'race_url'], dropna=False)
    rows = []
    for (race_date, race_url), g in groups:
        rid = sanitize_race_id(race_url if pd.notna(race_url) else '')
        date_folder = out_base / (str(race_date) if pd.notna(race_date) else 'unknown_date')
        date_folder.mkdir(parents=True, exist_ok=True)
        out_path = date_folder / f'predictions_{rid}.parquet'
        g.to_parquet(out_path, index=False)
        rows.append({'race_date': race_date, 'race_url': race_url, 'race_id': rid, 'rows': len(g), 'file': str(out_path.relative_to(ROOT))})

    summary = pd.DataFrame(rows).sort_values(['race_date', 'race_id'])
    summary_path = out_base / 'summary_by_race.csv'
    summary.to_csv(summary_path, index=False)
    print('Wrote', len(rows), 'per-race files. Summary at', summary_path)

    # Print top predictions per race (by win_prob) for quick inspection
    print('\nTop predictions per race (win_prob desc):')
    for _, r in summary.iterrows():
        p = ROOT / r['file']
        try:
            d = pd.read_parquet(p)
        except Exception:
            continue
        # show top 3 by win_prob if present, else by expected_value
        key = 'win_prob' if 'win_prob' in d.columns else ('expected_value' if 'expected_value' in d.columns else None)
        if key:
            top = d.sort_values(key, ascending=False if key == 'win_prob' else False).head(3)
        else:
            top = d.head(3)
        print(f"\n{r['race_date']} {r['race_id']} rows={r['rows']}")
        cols = ['horse_name', 'rank', 'odds', 'win_prob', 'expected_value', 'race_url']
        available = [c for c in cols if c in top.columns]
        print(top[available].to_string(index=False))


if __name__ == '__main__':
    main()
