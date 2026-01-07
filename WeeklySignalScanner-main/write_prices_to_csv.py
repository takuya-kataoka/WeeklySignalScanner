import os
import glob
import time
from datetime import datetime
import config

import pandas as pd
import yfinance as yf


def find_latest_csv():
    pattern = "outputs/results/jp_all_ma52_engulfing_*.csv"
    files = glob.glob(pattern)
    if not files:
        fallback = "outputs/results/jp_all_ma52_engulfing_2025-12-12.csv"
        if os.path.exists(fallback):
            return fallback
        raise FileNotFoundError("No jp_all_ma52_engulfing CSV found in outputs/results/")
    return max(files, key=os.path.getmtime)


def fetch_price(ticker: str):
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="1d", interval="1d")
        if not hist.empty:
            return float(hist['Close'].iloc[-1])
        info = t.info
        return info.get('regularMarketPrice') or info.get('previousClose')
    except Exception:
        return None


def main():
    csv_path = find_latest_csv()
    print(f"読み込み: {csv_path}")
    df = pd.read_csv(csv_path)
    if df.shape[0] == 0:
        print("CSVに銘柄が見つかりませんでした。終了します。")
        return

    # find ticker column
    candidates = ["ticker", "Ticker", "symbol", "Symbol", "code", "Code"]
    ticker_col = None
    for c in candidates:
        if c in df.columns:
            ticker_col = c
            break
    if ticker_col is None:
        ticker_col = df.columns[0]

    df = df.copy()
    df[ticker_col] = df[ticker_col].astype(str)

    prices = []
    total = len(df)
    for i, tk in enumerate(df[ticker_col].tolist()):
        print(f"({i+1}/{total}) 取得中: {tk}", end='')
        p = fetch_price(tk)
        prices.append(p)
        print(f" -> {p}")
        # be gentle to yfinance / Yahoo
        time.sleep(0.4)

    df['current_price'] = prices
    # sort ascending, put NaNs last
    df_sorted = df.sort_values(by='current_price', ascending=True, na_position='last')

    out_dir = 'outputs/results'
    os.makedirs(out_dir, exist_ok=True)
    # 日本語ファイル名で保存
    out_path = config.jp_filename('全銘柄_MA52_陽線包み_価格付き')
    df_sorted.to_csv(out_path, index=False)

    print(f"保存しました: {out_path} (行数: {len(df_sorted)})")


if __name__ == '__main__':
    main()
