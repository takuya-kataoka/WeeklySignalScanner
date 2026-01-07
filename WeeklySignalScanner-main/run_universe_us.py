#!/usr/bin/env python3
import argparse
import os
import csv
from datetime import datetime

def parse_args():
    p = argparse.ArgumentParser(description="Fetch/cache US stock tickers and run screener")
    p.add_argument('--fetch', action='store_true', help='Fetch and cache US tickers to local parquet files')
    p.add_argument('--scan', action='store_true', help='Run scan (can use cache)')
    p.add_argument('--market', type=str, default='custom', choices=['sp500', 'nasdaq100', 'custom'], 
                   help='Market to scan: sp500, nasdaq100, or custom ticker list')
    p.add_argument('--max-price', type=float, default=None, dest='max_price',
                   help='Maximum stock price filter (e.g., 2.0 for stocks under $2)')
    p.add_argument('--min-price', type=float, default=0.01, dest='min_price',
                   help='Minimum stock price filter to exclude penny stocks (default: 0.01)')
    p.add_argument('--batch-size', type=int, default=100, dest='batch_size')
    p.add_argument('--period', type=str, default='6mo', help='yfinance period for fetch (default 6mo)')
    p.add_argument('--interval', type=str, default='1d', help='yfinance interval')
    p.add_argument('--sleep', type=float, default=1.0, help='Sleep between batches (seconds)')
    p.add_argument('--retry', type=int, default=1, help='Retry count for downloads')
    p.add_argument('--cache-dir', type=str, default='data_us', help='Cache directory')
    p.add_argument('--use-cache', action='store_true', help='When scanning, use cached Parquet files')
    p.add_argument('--tickers', nargs='*', help='Custom list of tickers to scan')
    p.add_argument('--output-csv', type=str, default='results_us.csv', help='CSV file to write scan results')
    p.add_argument('--short-window', type=int, default=10)
    p.add_argument('--long-window', type=int, default=20)
    p.add_argument('--threshold', type=float, default=0.0)
    p.add_argument('--require-ma52', dest='require_ma52', action='store_true', default=True)
    p.add_argument('--no-require-ma52', dest='require_ma52', action='store_false')
    p.add_argument('--require-engulfing', dest='require_engulfing', action='store_true', default=True)
    p.add_argument('--no-require-engulfing', dest='require_engulfing', action='store_false')
    p.add_argument('--verbose', action='store_true')
    return p.parse_args()


def get_tickers_by_market(market, custom_tickers=None):
    """指定されたマーケットからティッカーリストを取得"""
    if market == 'custom' and custom_tickers:
        return custom_tickers
    elif market == 'sp500':
        from data_fetcher_us import get_sp500_tickers
        return get_sp500_tickers()
    elif market == 'nasdaq100':
        from data_fetcher_us import get_nasdaq100_tickers
        return get_nasdaq100_tickers()
    else:
        # デフォルトの主要銘柄リスト
        return [
            "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
            "AMD", "INTC", "NFLX", "CRM", "ORCL", "ADBE", "CSCO",
            "JPM", "BAC", "WFC", "GS", "MS", "V", "MA",
            "JNJ", "UNH", "PFE", "ABBV", "TMO", "MRK", "ABT",
            "WMT", "DIS", "NKE", "SBUX", "MCD", "HD", "LOW",
            "BA", "CAT", "GE", "MMM", "UPS", "HON",
            "XOM", "CVX", "COP", "SLB", "T", "VZ", "CMCSA"
        ]


def main():
    args = parse_args()

    # ティッカーリストを決定
    tickers = get_tickers_by_market(args.market, args.tickers)
    
    if args.verbose:
        print(f"Target market: {args.market}")
        print(f"Total tickers: {len(tickers)}")
    
    # 価格フィルタリング
    if args.max_price is not None:
        if args.verbose:
            print(f"\nFiltering tickers by price: ${args.min_price} - ${args.max_price}")
        from data_fetcher_us import filter_tickers_by_price
        tickers = filter_tickers_by_price(
            tickers, 
            max_price=args.max_price, 
            min_price=args.min_price,
            batch_size=args.batch_size,
            verbose=args.verbose
        )
        if args.verbose:
            print(f"After price filter: {len(tickers)} tickers")
        
        if not tickers:
            print("No tickers match the price criteria.")
            return

    if args.fetch:
        from data_fetcher_us import fetch_and_save_us_tickers
        print(f"Fetching {len(tickers)} US tickers...")
        fetch_and_save_us_tickers(
            tickers=tickers,
            batch_size=args.batch_size,
            period=args.period,
            interval=args.interval,
            out_dir=args.cache_dir,
            retry_count=args.retry,
            sleep_between_batches=args.sleep,
            verbose=args.verbose
        )
        print("Fetch complete!")

    if args.scan:
        print(f"Scanning {len(tickers)} US tickers...")
        
        if args.use_cache:
            from screener import scan_stocks_with_cache
            results = scan_stocks_with_cache(
                tickers,
                cache_dir=args.cache_dir,
                short_window=args.short_window,
                long_window=args.long_window,
                period=args.period,
                interval=args.interval,
                threshold=args.threshold,
                require_ma52=args.require_ma52,
                require_engulfing=args.require_engulfing
            )
        else:
            from screener import scan_stocks
            results = scan_stocks(
                tickers,
                short_window=args.short_window,
                long_window=args.long_window,
                period=args.period,
                interval=args.interval,
                threshold=args.threshold,
                require_ma52=args.require_ma52,
                require_engulfing=args.require_engulfing
            )

        # 結果をCSVに保存（日本語ファイル名）
        if results:
            import config
            os.makedirs('outputs/results', exist_ok=True)
            output_path = config.jp_filename('米国_MA52_陽線包み')

            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['ticker'])
                for ticker in results:
                    writer.writerow([ticker])

            print(f"\n=== Results saved to {output_path} ===")
            print(f"Found {len(results)} matching tickers:")
            for r in results:
                print(f"  - {r}")
        else:
            print("\n=== No matching tickers found ===")


if __name__ == "__main__":
    main()
