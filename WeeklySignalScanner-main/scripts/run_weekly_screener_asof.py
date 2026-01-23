#!/usr/bin/env python3
"""指定日時点の週足スクリーナーをローカルで実行してCSV出力するスクリプト

使い方例:
  cd WeeklySignalScanner-main/WeeklySignalScanner-main
  python3 scripts/run_weekly_screener_asof.py --date 2026-01-09 --tickers outputs/results/results_ma52_allranges_2025-12-03.csv
"""
import argparse
from datetime import datetime, timedelta
import os
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf
import pandas as pd

import config
from screener import check_signal


def load_tickers(path):
    # CSV with one column 'ticker' or plain lines; ignore header if present
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    tickers = []
    with open(path, newline='') as f:
        rdr = csv.reader(f)
        for row in rdr:
            if not row:
                continue
            t = row[0].strip()
            if t.lower() == 'ticker':
                continue
            if t:
                tickers.append(t)
    return tickers


def fetch_and_check(ticker, asof_date, require_ma52=True, require_engulfing=True):
    # fetch daily history up to asof_date (inclusive)
    # yfinance end is exclusive, so add one day
    end = (asof_date + timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        df = yf.Ticker(ticker).history(start='2000-01-01', end=end, interval='1d')
    except Exception:
        return None
    if df is None or getattr(df, 'empty', True):
        return None
    # pass df to check_signal which will resample to weekly
    try:
        ok = check_signal(ticker, data_df=df, require_ma52=require_ma52, require_engulfing=require_engulfing)
    except Exception:
        return None
    if not ok:
        return None

    # compute weekly last close to record price
    try:
        d = df.copy()
        if not isinstance(d.index, pd.DatetimeIndex):
            d.index = pd.to_datetime(d.index)
        weekly = d.resample('W-FRI').agg({'Close': 'last'})
        weekly.dropna(inplace=True)
        if weekly.empty:
            return (ticker, None)
        last_close = float(weekly['Close'].iloc[-1])
    except Exception:
        last_close = None

    return (ticker, last_close)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--date', required=True, help='対象日（YYYY-MM-DD）')
    p.add_argument('--tickers', required=True, help='ティッカー一覧CSV（1列またはヘッダあり）')
    p.add_argument('--outdir', default='outputs/results', help='出力ディレクトリ')
    p.add_argument('--workers', type=int, default=8, help='同時実行数')
    p.add_argument('--limit', type=int, default=None, help='ティッカー上限（テスト用）')
    p.add_argument('--relaxed', action='store_true', help='包み足判定を緩和する')
    args = p.parse_args()

    asof_date = datetime.strptime(args.date, '%Y-%m-%d')
    tickers = load_tickers(args.tickers)
    if args.limit:
        tickers = tickers[:args.limit]

    os.makedirs(args.outdir, exist_ok=True)

    results = []
    start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(fetch_and_check, t, asof_date, require_ma52=True, require_engulfing=not args.relaxed): t for t in tickers}
        for fut in as_completed(futures):
            t = futures[fut]
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception:
                # ignore per-ticker failures
                continue

    elapsed = time.time() - start
    # save CSV
    from datetime import datetime as _dt
    out_name = config.jp_filename('全銘柄_MA52_陽線包み', date=asof_date)
    out_path = out_name
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['ticker', 'price'])
        for t, price in sorted(results):
            w.writerow([t, '' if price is None else price])

    print(f"Done. AsOf={args.date} Checked={len(tickers)} Hits={len(results)} Elapsed={elapsed:.1f}s -> {out_path}")


if __name__ == '__main__':
    main()
