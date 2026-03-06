#!/usr/bin/env python3
"""
Runner to perform full-range fetch using `data_fetcher.fetch_and_save_tickers`.
Designed to be launched in background from the workspace.
"""
import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import data_fetcher

def main():
    # parameters: you can tune period/batch_size to control runtime
    start = 1000
    end = 9999
    batch_size = 200
    period = '3y'
    interval = '1d'
    out_dir = str(repo_root / 'data')
    data_fetcher.fetch_and_save_tickers(start=start, end=end, batch_size=batch_size, period=period, interval=interval, out_dir=out_dir, retry_count=2, sleep_between_batches=0.5, allow_excluded=False, verbose=True)

if __name__ == '__main__':
    main()
