#!/usr/bin/env python3
import time
import re
import csv
import argparse
import yfinance as yf

INPUT = 'WeeklySignalScanner-main/outputs/failed_tickers_2025-12-26.txt'
OUTCSV = 'WeeklySignalScanner-main/outputs/verified_failed_tickers_2025-12-26.csv'


def load_tickers_from_report(path):
    with open(path, 'r', encoding='utf-8') as f:
        txt = f.read()
    # find all patterns like 1234.T
    tickers = re.findall(r"\d{4}\.T", txt)
    # unique and preserve order
    seen = set()
    ordered = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            ordered.append(t)
    return ordered


def check_ticker_exists(ticker, timeout=10):
    start = time.time()
    try:
        # Try short history first
        df = yf.Ticker(ticker).history(period='1mo')
        elapsed = time.time() - start
        if df is None:
            return False, 0, elapsed, 'no-data'
        rows = len(df.dropna(how='all'))
        if rows == 0:
            return False, 0, elapsed, 'empty-history'
        return True, rows, elapsed, ''
    except Exception as e:
        return False, 0, time.time() - start, f'error:{e}'


def main(input_path=INPUT, out_csv=OUTCSV, sleep=0.05, limit=None):
    tickers = load_tickers_from_report(input_path)
    if limit is not None:
        tickers = tickers[:limit]

    os_out = []
    total_start = time.time()
    with open(out_csv, 'w', newline='', encoding='utf-8') as csvf:
        w = csv.writer(csvf)
        w.writerow(['ticker', 'exists', 'rows', 'elapsed_s', 'note'])
        for idx, t in enumerate(tickers, start=1):
            exists, rows, elapsed, note = check_ticker_exists(t)
            w.writerow([t, 'yes' if exists else 'no', rows, f"{elapsed:.2f}", note])
            print(f"[{idx}/{len(tickers)}] {t} -> {'yes' if exists else 'no'} ({rows}) {note}")
            time.sleep(sleep)
    total_elapsed = time.time() - total_start
    print(f"Done. Checked {len(tickers)} tickers in {total_elapsed:.1f}s. Results -> {out_csv}")


if __name__ == '__main__':
    import sys
    p = argparse.ArgumentParser()
    p.add_argument('--input', default=INPUT)
    p.add_argument('--output', default=OUTCSV)
    p.add_argument('--sleep', type=float, default=0.05)
    p.add_argument('--limit', type=int, default=None, help='Limit number of tickers to check (for testing)')
    args = p.parse_args()
    main(input_path=args.input, out_csv=args.output, sleep=args.sleep, limit=args.limit)
