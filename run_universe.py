#!/usr/bin/env python3
import argparse
import os
import csv

def parse_args():
    p = argparse.ArgumentParser(description="Fetch/cache JP tickers and run screener")
    p.add_argument('--fetch', action='store_true', help='Fetch and cache tickers to local parquet files')
    p.add_argument('--scan', action='store_true', help='Run scan (can use cache)')
    p.add_argument('--start', type=int, default=7200, help='Start code (4-digit)')
    p.add_argument('--end', type=int, default=7210, help='End code (4-digit)')
    p.add_argument('--batch-size', type=int, default=200, dest='batch_size')
    p.add_argument('--period', type=str, default='6mo', help='yfinance period for fetch (default 6mo)')
    p.add_argument('--interval', type=str, default='1d', help='yfinance interval')
    p.add_argument('--sleep', type=float, default=1.0, help='Sleep between batches (seconds)')
    p.add_argument('--retry', type=int, default=1, help='Retry count for downloads')
    p.add_argument('--cache-dir', type=str, default='data', help='Cache directory')
    p.add_argument('--use-cache', action='store_true', help='When scanning, use cached Parquet files')
    p.add_argument('--tickers', nargs='*', help='List of tickers to scan (overrides start/end when provided)')
    p.add_argument('--output-csv', type=str, default='results.csv', help='CSV file to write scan results')
    p.add_argument('--short-window', type=int, default=10)
    p.add_argument('--long-window', type=int, default=20)
    p.add_argument('--threshold', type=float, default=0.0)
    p.add_argument('--verbose', action='store_true')
    return p.parse_args()


def main():
    args = parse_args()

    if args.fetch:
        from data_fetcher import fetch_and_save_tickers
        fetch_and_save_tickers(start=args.start, end=args.end, batch_size=args.batch_size, period=args.period, interval=args.interval, out_dir=args.cache_dir, retry_count=args.retry, sleep_between_batches=args.sleep, verbose=args.verbose)

    if args.scan:
        if args.tickers and len(args.tickers) > 0:
            tickers = args.tickers
        else:
            # if not provided tickers, build from start..end
            tickers = [f"{i:04d}.T" for i in range(args.start, args.end + 1)]

        if args.use_cache:
            from screener import scan_stocks_with_cache
            results = scan_stocks_with_cache(tickers, cache_dir=args.cache_dir, short_window=args.short_window, long_window=args.long_window, period=args.period, interval=args.interval, threshold=args.threshold)
        else:
            from screener import scan_stocks
            results = scan_stocks(tickers, short_window=args.short_window, long_window=args.long_window, period=args.period, interval=args.interval, threshold=args.threshold)

        # write results
        if args.verbose:
            print(f"Writing {len(results)} results to {args.output_csv}")

        with open(args.output_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ticker'])
            for r in results:
                writer.writerow([r])

        if args.verbose:
            print('Done')


if __name__ == '__main__':
    main()
