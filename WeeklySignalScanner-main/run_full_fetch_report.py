#!/usr/bin/env python3
import time
import os
import argparse
import pandas as pd

from data_fetcher import fetch_and_save_tickers, EXCLUDED_TICKERS


def main(start=1000, end=9999, batch_size=200, out_dir='data', do_fetch=True, verbose=True):
    os.makedirs(out_dir, exist_ok=True)
    # build list of codes the current fetcher will attempt (respects EXCLUDED_TICKERS)
    all_codes = [f"{i:04d}.T" for i in range(start, end + 1) if f"{i:04d}.T" not in EXCLUDED_TICKERS]

    start_time = time.time()
    if do_fetch:
        fetch_and_save_tickers(start=start, end=end, batch_size=batch_size, out_dir=out_dir, allow_excluded=False, verbose=verbose)

    # after fetch (or without fetch), check which tickers are missing or have no valid Close
    failed = []
    for t in all_codes:
        path = os.path.join(out_dir, f"{t}.parquet")
        if not os.path.exists(path):
            failed.append(t)
            continue
        try:
            df = pd.read_parquet(path)
            if 'Close' not in df.columns or df.dropna(subset=['Close']).empty:
                failed.append(t)
        except Exception:
            failed.append(t)

    elapsed = time.time() - start_time

    # print results
    print(f"Attempted tickers: {len(all_codes)}")
    print(f"Succeeded: {len(all_codes) - len(failed)}")
    print(f"Failed: {len(failed)}")
    if failed:
        print('\nComma-separated failed tickers:')
        print(','.join(failed))
    print(f"\nElapsed time: {elapsed:.1f} seconds")


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Run full fetch and report failures')
    p.add_argument('--start', type=int, default=1000)
    p.add_argument('--end', type=int, default=9999)
    p.add_argument('--batch-size', type=int, default=200)
    p.add_argument('--out-dir', type=str, default='data')
    p.add_argument('--no-fetch', dest='do_fetch', action='store_false', help='Do not perform network fetch; only check cache')
    p.add_argument('--quiet', dest='verbose', action='store_false', help='Suppress verbose fetch output')
    args = p.parse_args()
    main(start=args.start, end=args.end, batch_size=args.batch_size, out_dir=args.out_dir, do_fetch=args.do_fetch, verbose=args.verbose)
