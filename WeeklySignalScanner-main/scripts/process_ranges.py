#!/usr/bin/env python3
"""
範囲ごとにデータ取得→検証→不足分リトライ→MA52抽出を自動実行するスクリプト

使い方:
    source .venv/bin/activate
    python3 scripts/process_ranges.py

設定は下部の DEFAULTS を変更してください。
"""
import os
import sys
import time
from datetime import datetime

import pathlib

# ensure project root is on sys.path so sibling modules can be imported when running this script
project_root = str(pathlib.Path(__file__).resolve().parents[1])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd

from data_fetcher import fetch_and_save_tickers
from screener import scan_above_ma52_with_cache
import yfinance as yf


DEFAULTS = {
    'period': '2y',
    'interval': '1wk',
    'batch_size': 200,
    'sleep': 0.8,
    'retry': 2,
    'cache_dir': 'data',
    'subrange_step': 1000,
    'start': 3000,
    'end': 9999,
}


def verify_range(start, end, cache_dir):
    ok = []
    bad = []
    missing = []
    for i in range(start, end + 1):
        t = f"{i:04d}.T"
        path = os.path.join(cache_dir, f"{t}.parquet")
        if not os.path.exists(path):
            missing.append(t)
            continue
        try:
            df = pd.read_parquet(path)
            closes = df['Close'].dropna() if 'Close' in df.columns else pd.Series(dtype=float)
            n = len(closes)
            if n >= 52:
                ok.append((t, n))
            else:
                bad.append((t, n))
        except Exception:
            bad.append((t, 0))
    return ok, bad, missing


def retry_bad(bad_list, cache_dir, max_attempts=3, sleep_between=0.8):
    """個別リトライで Parquet を上書きする。"""
    for t in bad_list:
        attempts = 0
        success = False
        while attempts < max_attempts and not success:
            attempts += 1
            try:
                df = yf.Ticker(t).history(period=DEFAULTS['period'], interval=DEFAULTS['interval'])
                if df is None or getattr(df, 'empty', True):
                    time.sleep(sleep_between)
                    continue
                if 'Close' not in df.columns or df['Close'].dropna().empty:
                    time.sleep(sleep_between)
                    continue
                cols = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in df.columns]
                path = os.path.join(cache_dir, f"{t}.parquet")
                df[cols].to_parquet(path)
                success = True
            except Exception:
                time.sleep(sleep_between)
        if not success:
            # leave as-is (will remain bad/missing)
            pass


def process_subrange(s, e, cfg):
    now = datetime.today().strftime('%Y-%m-%d')
    cache_dir = cfg['cache_dir']

    print(f"\n=== Processing {s}-{e} ===")

    # 1) Fetch batch
    fetch_and_save_tickers(start=s, end=e, batch_size=cfg['batch_size'], period=cfg['period'], interval=cfg['interval'], out_dir=cache_dir, retry_count=cfg['retry'], sleep_between_batches=cfg['sleep'], verbose=True)

    # 2) Verify
    ok, bad, missing = verify_range(s, e, cache_dir)
    print(f"Verify {s}-{e}: OK={len(ok)}, BAD={len(bad)}, MISSING={len(missing)}")
    ver_path = f"verification_{s}_{e}.txt"
    with open(ver_path, 'w') as f:
        f.write(f"ok_count={len(ok)}\n")
        f.write('\n'.join([f"{t},{n}" for t, n in ok]))
        f.write('\n---BAD---\n')
        f.write('\n'.join([f"{t},{n}" for t, n in bad]))
        f.write('\n---MISSING---\n')
        f.write('\n'.join(missing))
    print(f"Wrote {ver_path}")

    # 3) Retry bad (individual)
    if bad:
        bad_tickers = [t for t, n in bad]
        print(f"Retrying {len(bad_tickers)} bad tickers individually")
        retry_bad(bad_tickers, cache_dir, max_attempts=3, sleep_between=cfg['sleep'])

    # 4) Re-verify and run MA52 scan on OK only
    ok2, bad2, missing2 = verify_range(s, e, cache_dir)
    ok_tickers = [t for t, n in ok2]
    print(f"After retry Verify {s}-{e}: OK={len(ok2)}, BAD={len(bad2)}, MISSING={len(missing2)}")

    # Run MA52 scan on OK tickers
    if ok_tickers:
        print(f"Running MA52 scan on {len(ok_tickers)} tickers")
        results = scan_above_ma52_with_cache(ok_tickers, cache_dir=cache_dir)
    else:
        results = []

    out_csv = f"results_ma52_{s}_{e}_{now}.csv"
    import csv
    with open(out_csv, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['ticker'])
        for r in results:
            writer.writerow([r])
    print(f"Found {len(results)} MA52 matches, wrote {out_csv}")

    # return summary
    return {
        'range': f"{s}-{e}",
        'ok': len(ok2),
        'bad': len(bad2),
        'missing': len(missing2),
        'ma52_matches': len(results),
        'ver_file': ver_path,
        'out_csv': out_csv,
    }


def main():
    cfg = DEFAULTS.copy()
    start = cfg['start']
    end = cfg['end']
    step = cfg['subrange_step']

    summaries = []
    for s in range(start, end + 1, step):
        e = min(s + step - 1, end)
        summary = process_subrange(s, e, cfg)
        summaries.append(summary)

    # write overall summary
    now = datetime.today().strftime('%Y-%m-%d')
    with open(f'process_summary_{now}.csv', 'w', newline='') as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=['range', 'ok', 'bad', 'missing', 'ma52_matches', 'ver_file', 'out_csv'])
        writer.writeheader()
        for s in summaries:
            writer.writerow(s)

    print('\nAll done. Summary written to', f'process_summary_{now}.csv')


if __name__ == '__main__':
    main()
