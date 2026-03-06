#!/usr/bin/env python3
"""
Scan local `data/*.parquet` cache and produce a CSV report with basic health metrics.
"""
from pathlib import Path
import pandas as pd
from datetime import datetime
import csv

# The project layout has two nested folders; choose workspace root as parent of the outer folder
ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / 'data'
OUT_DIR = ROOT / 'outputs' / 'results'
OUT_DIR.mkdir(parents=True, exist_ok=True)

def analyze_parquet(p: Path):
    try:
        df = pd.read_parquet(p)
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        # monthly bars
        m = df.resample('M').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
        valid_months = len(m.dropna(subset=['Close']))
        latest = None
        try:
            latest = df.index.max()
        except Exception:
            latest = None
        return {
            'ticker': p.stem,
            'rows': len(df),
            'monthly_bars': valid_months,
            'latest_date': latest.strftime('%Y-%m-%d') if latest is not None else '',
        }
    except Exception as e:
        return {'ticker': p.stem, 'rows': 0, 'monthly_bars': 0, 'latest_date': '', 'error': str(e)}

def main():
    files = sorted(DATA_DIR.glob('*.parquet'))
    print('Found', len(files), 'parquet files in', DATA_DIR)
    rows = []
    for p in files:
        rows.append(analyze_parquet(p))

    now = datetime.now().strftime('%Y-%m-%d')
    out_path = OUT_DIR / f'cache_scan_report_{now}.csv'
    keys = ['ticker','rows','monthly_bars','latest_date']
    with open(out_path, 'w', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k,'') for k in keys})

    # summary
    total = len(rows)
    zero_month = sum(1 for r in rows if r.get('monthly_bars',0) < 2)
    print(f"Total cached tickers: {total}")
    print(f"Tickers with <2 monthly bars: {zero_month}")
    print('Saved report to', out_path)

if __name__ == '__main__':
    main()
