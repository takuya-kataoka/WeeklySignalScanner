import os
import glob
from datetime import datetime
import time

import pandas as pd
import yfinance as yf


def find_latest_csv():
    # Prefer the original scan results file (exclude files that already contain prices)
    pattern = "outputs/results/jp_all_ma52_engulfing_*.csv"
    files = [f for f in glob.glob(pattern) if 'with_prices' not in os.path.basename(f)]
    if not files:
        # fallback: try any matching file
        files = glob.glob(pattern)
    if not files:
        fallback = "outputs/results/jp_all_ma52_engulfing_2025-12-12.csv"
        if os.path.exists(fallback):
            return fallback
        raise FileNotFoundError("No jp_all_ma52_engulfing CSV found in outputs/results/")
    # Return the most recent base result file
    return max(files, key=os.path.getmtime)


def batch_fetch_prices(tickers, batch_size=50, pause=1.0):
    prices = {}
    total = len(tickers)
    for i in range(0, total, batch_size):
        batch = tickers[i:i+batch_size]
        tickers_str = ' '.join(batch)
        print(f"バッチ取得 {i+1}-{min(i+batch_size, total)} / {total}: {len(batch)} 銘柄")
        try:
            data = yf.download(batch, period='1d', interval='1d', threads=True, progress=False)
            # data['Close'] may be DataFrame or Series
            if 'Close' in data:
                closes = data['Close']
            else:
                closes = data

            if isinstance(closes, pd.Series):
                # single column
                for tk in batch:
                    val = closes.get(tk)
                    prices[tk] = float(val) if pd.notna(val) else None
            else:
                # DataFrame with tickers as columns
                last = closes.iloc[-1]
                for tk in batch:
                    val = last.get(tk)
                    prices[tk] = float(val) if pd.notna(val) else None
        except Exception as e:
            print(f"バッチ取得で例外: {e}")
            # fallback per-ticker
            for tk in batch:
                try:
                    t = yf.Ticker(tk)
                    hist = t.history(period='1d', interval='1d')
                    prices[tk] = float(hist['Close'].iloc[-1]) if not hist.empty else None
                except Exception:
                    prices[tk] = None
                time.sleep(0.2)

        time.sleep(pause)

    return prices


def main():
    import config

    csv_path = find_latest_csv()
    print(f"読み込み: {csv_path}")
    df = pd.read_csv(csv_path)
    if df.shape[0] == 0:
        print("CSVに銘柄が見つかりませんでした。終了します。")
        return

    # detect ticker column
    candidates = ["ticker", "Ticker", "symbol", "Symbol", "code", "Code"]
    ticker_col = None
    for c in candidates:
        if c in df.columns:
            ticker_col = c
            break
    if ticker_col is None:
        ticker_col = df.columns[0]

    tickers = df[ticker_col].astype(str).tolist()
    # 除外銘柄をフィルタ
    tickers = [t for t in tickers if t not in config.EXCLUDE_TICKERS]
    print(f"対象銘柄数: {len(tickers)} (除外済み除く)")

    prices = batch_fetch_prices(tickers, batch_size=50, pause=0.8)

    df = df.copy()
    df['current_price'] = df[ticker_col].map(prices)

    df_sorted = df.sort_values(by='current_price', ascending=True, na_position='last')

    out_dir = 'outputs/results'
    os.makedirs(out_dir, exist_ok=True)
    date = datetime.now()
    # 日本語ファイル名で保存
    out_path = config.jp_filename('全銘柄_MA52_陽線包み_価格付き', date)
    df_sorted.to_csv(out_path, index=False)

    print(f"保存しました: {out_path} (行数: {len(df_sorted)})")


if __name__ == '__main__':
    main()
