import os
import time
import yfinance as yf
import pandas as pd

# 今後取得しない除外銘柄
EXCLUDED_TICKERS = {f"{code}.T" for code in [1326, 1543, 1555, 1586, 1593, 1618, 1621, 1672, 1674, 1679, 1736, 1795, 1807, 2012, 2013, 1325, 2050, 2250, 1656]}


def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def fetch_and_save_tickers(start=1000, end=9999, batch_size=200, period='6mo', interval='1d', out_dir='data', retry_count=2, sleep_between_batches=1.0, verbose=False):
    """
    指定範囲のティッカー（4桁コードに .T を付与）をバッチで取得して、各ティッカーごとに Parquet ファイルとして保存します。

    デフォルトで半年分（period='6mo'）を取得します。
    """
    _ensure_dir(out_dir)

    all_codes = [f"{i:04d}.T" for i in range(start, end + 1) if f"{i:04d}.T" not in EXCLUDED_TICKERS]
    total = len(all_codes)
    total_batches = (total - 1) // batch_size + 1

    for batch_idx, i in enumerate(range(0, total, batch_size), start=1):
        batch = all_codes[i:i+batch_size]
        if verbose:
            print(f"Fetching batch {batch_idx}/{total_batches} (size={len(batch)})")

        attempt = 0
        df = None
        while attempt <= retry_count:
            try:
                df = yf.download(batch, period=period, interval=interval, progress=False, group_by='ticker', auto_adjust=False)
                break
            except Exception as e:
                attempt += 1
                if attempt > retry_count:
                    if verbose:
                        print(f"batch download failed after {attempt} attempts: {e}")
                else:
                    wait = sleep_between_batches * (2 ** (attempt - 1))
                    if verbose:
                        print(f"batch download error, retrying after {wait}s: {e}")
                    time.sleep(wait)

        # If batch df is empty, fallback to per-ticker
        if df is None or (hasattr(df, 'empty') and df.empty):
            if verbose:
                print("Batch empty — falling back to per-ticker fetch")
            for t in batch:
                try:
                    single = yf.Ticker(t).history(period=period, interval=interval)
                    if single is None or getattr(single, 'empty', True):
                        if verbose:
                            print(f"{t}: no data")
                        continue
                    # keep common columns
                    cols = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in single.columns]
                    valid = single.dropna(subset=['Close']) if 'Close' in single.columns else single
                    if valid is None or getattr(valid, 'empty', True):
                        if verbose:
                            print(f"{t}: no valid Close values, skipping save")
                        continue
                    path = os.path.join(out_dir, f"{t}.parquet")
                    single[cols].to_parquet(path)
                    if verbose:
                        print(f"Saved {t} -> {path}")
                except Exception as e:
                    if verbose:
                        print(f"{t}: fetch error {e}")
                time.sleep(sleep_between_batches)
            continue

        # Parse batch df and save per-ticker files
        for t in batch:
            try:
                series_df = None
                if isinstance(df.columns, pd.MultiIndex):
                    # check ('Close', t) or (t, 'Close') shapes or similar
                    if ('Close', t) in df.columns:
                        series_df = df.xs(t, level=1, axis=1)
                    elif (t, 'Close') in df.columns:
                        series_df = df.xs(t, level=0, axis=1)
                    else:
                        # search columns that include ticker
                        cols = [c for c in df.columns if t in str(c)]
                        if cols:
                            # select all columns for that ticker
                            series_df = df[cols]
                else:
                    # single dataframe returned — try to find columns that include ticker
                    possible_cols = [c for c in df.columns if t in str(c)]
                    if possible_cols:
                        series_df = df[possible_cols]

                if series_df is None or getattr(series_df, 'empty', True):
                    # fallback single ticker fetch
                    single = yf.Ticker(t).history(period=period, interval=interval)
                    if single is None or getattr(single, 'empty', True):
                        if verbose:
                            print(f"{t}: no data (batch+single)")
                        continue
                    series_df = single

                # only save if there are valid Close values
                if 'Close' not in series_df.columns or series_df.dropna(subset=['Close']).empty:
                    if verbose:
                        print(f"{t}: no valid Close values, skipping save")
                    continue

                path = os.path.join(out_dir, f"{t}.parquet")
                cols = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in series_df.columns]
                series_df[cols].to_parquet(path)
                if verbose:
                    print(f"Saved {t} -> {path}")
            except Exception as e:
                if verbose:
                    print(f"{t}: error saving - {e}")
        time.sleep(sleep_between_batches)


def load_ticker_from_cache(ticker, cache_dir='data'):
    path = os.path.join(cache_dir, f"{ticker}.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        return df
    except Exception:
        return None
