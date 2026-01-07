#!/usr/bin/env python3
"""
Add current prices to a tickers CSV and write a sorted version.
Usage:
  python scripts/add_prices_and_sort.py /path/to/file.csv

Output files will be written next to inputs with suffixes:
  *_with_prices_YYYY-MM-DD.csv and *_with_prices_YYYY-MM-DD_sorted.csv
"""
import sys, os
import pandas as pd
import yfinance as yf
from datetime import datetime


def get_price_from_cache(ticker, data_dir='data'):
    fn = os.path.join(data_dir, f"{ticker}.parquet")
    if os.path.exists(fn):
        try:
            df = pd.read_parquet(fn)
            if 'Close' in df.columns:
                series = df['Close'].dropna()
                if len(series):
                    return float(series.iloc[-1])
        except Exception:
            return None
    return None


def get_price_yf(ticker):
    try:
        t = yf.Ticker(ticker)
        h = t.history(period='7d', interval='1d')
        if h is None or h.empty:
            return None
        h = h.dropna(subset=['Close'])
        if h.empty:
            return None
        return float(h['Close'].iloc[-1])
    except Exception:
        return None


def main():
    if len(sys.argv) < 2:
        print('Usage: add_prices_and_sort.py path/to/input.csv')
        sys.exit(1)
    infile = sys.argv[1]
    if not os.path.exists(infile):
        print('Input file not found:', infile)
        sys.exit(2)

    df = pd.read_csv(infile, header=0)
    # try to find ticker column
    if 'ticker' in df.columns:
        tickers = df['ticker'].astype(str).tolist()
    else:
        # assume single-column file
        tickers = df.iloc[:,0].astype(str).tolist()

    out_rows = []
    for t in tickers:
        t = t.strip()
        price = get_price_from_cache(t)
        source = 'cache' if price is not None else 'yfinance'
        if price is None:
            price = get_price_yf(t)
            if price is None:
                source = 'none'
        out_rows.append({'ticker': t, 'current_price': price, 'price_source': source})
        print(f"{t}: {price} ({source})")

    out_df = pd.DataFrame(out_rows)

    # derive date suffix from input filename if possible
    base = os.path.basename(infile)
    # try to find YYYY-MM-DD in filename
    import re
    m = re.search(r"(\d{4}-\d{2}-\d{2})", base)
    date_str = m.group(1) if m else datetime.now().strftime('%Y-%m-%d')

    base_noext = os.path.splitext(infile)[0]
    out_dir = os.path.dirname(infile)
    out_name = os.path.join(out_dir, f"{base_noext}_with_prices_{date_str}.csv")
    # if base_noext contains path, adjust
    if os.path.dirname(out_name) == '':
        out_name = f"{base_noext}_with_prices_{date_str}.csv"

    out_df.to_csv(out_name, index=False)
    print('Wrote:', out_name)

    sorted_df = out_df.sort_values(by='current_price', ascending=False, na_position='last').reset_index(drop=True)
    sorted_name = os.path.splitext(out_name)[0] + '_sorted.csv'
    sorted_df.to_csv(sorted_name, index=False)
    print('Wrote sorted:', sorted_name)

if __name__ == '__main__':
    main()
