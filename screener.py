import yfinance as yf
import pandas as pd
from utils import calculate_ma

def check_signal(ticker, short_window=10, long_window=20, period="2y", interval="1wk", threshold=0.0, data_df=None):
    """
    指定パラメータでティッカーのシグナルを判定する。

    - short_window: 短期移動平均の窓 (例: 10)
    - long_window: 長期移動平均の窓 (例: 20)
    - period: データ取得期間 (yfinance に渡す文字列、例: "2y")
    - interval: データ間隔 (例: "1wk")
    - threshold: 判定の閾値（比率）。最新の短期MAが長期MAより threshold 以上上回る必要がある（例: 0.01 = 1%）
    """
    if short_window >= long_window:
        print(f"{ticker}: short_window({short_window}) must be < long_window({long_window})")
        return False

    # If a DataFrame is provided (cache), use it; otherwise fetch from network
    if data_df is None:
        data = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False)

        if data is None or getattr(data, 'empty', True) or len(data) < 2:
            print(f"{ticker}: データが足りません")
            return False
    else:
        data = data_df.copy()

    data.dropna(inplace=True)
    data.reset_index(drop=True, inplace=True)

    # 新判定ロジック:
    # - 週足で「陽線包み足 (bullish engulfing)」が発生していること
    # - かつ最新終値が週足の52週移動平均 (MA52) 以上であること

    # 52週移動平均を追加
    data["MA52"] = calculate_ma(data["Close"], 52)

    # 直近2本のローソク足が必要
    if len(data) < 2:
        print(f"{ticker}: データが足りません（2週未満）")
        return False

    # 最新と1つ前の足
    prev = data.iloc[-2]
    curr = data.iloc[-1]

    # 必要な値が存在するか
    try:
        def _to_float(val):
            # val may be a scalar or a single-element Series; handle both
            if isinstance(val, (list, tuple)):
                val = val[0]
            try:
                import pandas as _pd
                if isinstance(val, _pd.Series):
                    return float(val.iloc[0])
            except Exception:
                pass
            return float(val)

        prev_open = _to_float(prev["Open"])
        prev_close = _to_float(prev["Close"])
        curr_open = _to_float(curr["Open"])
        curr_close = _to_float(curr["Close"])
    except Exception:
        print(f"{ticker}: ローソク足データが不十分")
        return False

    # MA52 の確認
    ma52_latest = data["MA52"].iloc[-1]
    if pd.isna(ma52_latest):
        print(f"{ticker}: MA52が計算できません（データ不足）")
        return False

    # bullish engulfing 判定:
    # - 前の足が陰線 (prev_close < prev_open)
    # - 今の足が陽線 (curr_close > curr_open)
    # - 今の実体が前の実体を包んでいる: curr_open <= prev_close and curr_close >= prev_open
    is_prev_bear = prev_close < prev_open
    is_curr_bull = curr_close > curr_open
    engulfs = (curr_open <= prev_close) and (curr_close >= prev_open)

    if is_prev_bear and is_curr_bull and engulfs:
        # 価格がMA52以上かチェック
        if curr_close >= ma52_latest:
            print(f"{ticker}: シグナル検出（陽線包み足） price={curr_close:.2f} MA52={ma52_latest:.2f}")
            return True
        else:
            print(f"{ticker}: 陽線包み足だが価格がMA52未満 price={curr_close:.2f} MA52={ma52_latest:.2f}")
            return False

    # 条件未達
    return False


def scan_stocks(tickers, short_window=10, long_window=20, period="2y", interval="1wk", threshold=0.0):
    """指定したティッカー群を順にチェックし、シグナルが出た銘柄のリストを返す。

    scan_stocks(..., short_window=10, long_window=20, period='2y', interval='1wk', threshold=0.0)
    """
    results = []
    for t in tickers:
        try:
            ok = check_signal(t, short_window=short_window, long_window=long_window, period=period, interval=interval, threshold=threshold)
            if ok:
                results.append(t)
        except Exception as e:
            print(f"{t}: エラー - {e}")
    return results


def scan_stocks_with_cache(tickers, cache_dir='data', short_window=10, long_window=20, period="2y", interval="1wk", threshold=0.0):
    """Scan using locally cached per-ticker Parquet files if available.
    If a ticker has no cache file, it will be skipped.
    """
    from data_fetcher import load_ticker_from_cache
    results = []
    for t in tickers:
        try:
            df = load_ticker_from_cache(t, cache_dir=cache_dir)
            if df is None:
                print(f"{t}: cache not found, skipping")
                continue
            ok = check_signal(t, short_window=short_window, long_window=long_window, period=period, interval=interval, threshold=threshold, data_df=df)
            if ok:
                results.append(t)
        except Exception as e:
            print(f"{t}: エラー - {e}")
    return results


def generate_jp_tickers_under_price(max_price=1000, start=1000, end=9999, batch_size=200, period='1mo', interval='1d', retry_count=2, sleep_between_batches=1.0, backoff=2.0, verbose=False):
    """
    東京市場の4桁コード（`{start}`..`{end}`）に `.T` を付けたティッカー群をバッチで取得して、
    最新終値が `max_price` 以下の銘柄リストを返す。

    注意:
    - すべての証券コードが存在するわけではないため、多数の無効ティッカーが含まれます。
    - 大量取得は時間がかかり、yfinance / Yahoo 側の制限にかかる可能性があります。
    - `batch_size` を小さくして負荷を下げること。
    """
    results = []
    failures = []
    all_codes = [f"{i:04d}.T" for i in range(start, end + 1)]

    total_batches = (len(all_codes) - 1) // batch_size + 1
    for batch_idx, i in enumerate(range(0, len(all_codes), batch_size), start=1):
        batch = all_codes[i:i+batch_size]
        if verbose:
            print(f"Checking batch {batch_idx}/{total_batches} (size={len(batch)})")

        # download with retries
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
                        print(f"batch download error after {attempt} attempts: {e}")
                        # batch download ultimately failed — do not mark all as failed yet,
                        # fall back to per-ticker downloads below and record failures there.
                        if verbose:
                            print("Batch download failed; will attempt per-ticker fallback")
                else:
                    wait = sleep_between_batches * (backoff ** (attempt - 1))
                    if verbose:
                        print(f"download failed (attempt {attempt}/{retry_count}), retrying after {wait}s: {e}")
                    import time
                    time.sleep(wait)

        # If df is None or empty, try per-ticker fallback downloads
        if df is None or (hasattr(df, 'empty') and df.empty):
            if verbose:
                print("Batch empty or failed — falling back to per-ticker downloads")
            for t in batch:
                per_attempt = 0
                per_df = None
                while per_attempt <= retry_count:
                    try:
                        # Use Ticker.history as a more reliable per-symbol fetch
                        per_df = yf.Ticker(t).history(period=period, interval=interval)
                        break
                    except Exception as e:
                        per_attempt += 1
                        if per_attempt > retry_count:
                            if verbose:
                                print(f"single download failed for {t}: {e}")
                            failures.append(t)
                        else:
                            wait = sleep_between_batches * (backoff ** (per_attempt - 1))
                            if verbose:
                                print(f"single download retry {per_attempt} for {t} after {wait}s: {e}")
                            import time
                            time.sleep(wait)

                if per_df is None or (hasattr(per_df, 'empty') and per_df.empty):
                    continue

                # parse single-ticker df
                try:
                    if 'Close' in per_df.columns:
                        series = per_df['Close']
                    else:
                        possible_cols = [c for c in per_df.columns if 'Close' in str(c)]
                        series = per_df[possible_cols[0]] if possible_cols else None

                    if series is None or series.dropna().empty:
                        failures.append(t)
                        continue

                    close_val = float(series.dropna().iloc[-1])
                    if close_val <= max_price:
                        if verbose:
                            print(f"{t}: price={close_val} <= {max_price} -> added (single download)")
                        results.append(t)
                except Exception:
                    failures.append(t)
            # move to next batch
            time.sleep(sleep_between_batches)
            continue

        for t in batch:
            close_val = None
            series = None
            try:
                # MultiIndex (field, ticker) 形式の場合
                if isinstance(df.columns, pd.MultiIndex):
                    # Try common MultiIndex shapes: ('Close', ticker) or (ticker, 'Close')
                    if ('Close', t) in df.columns:
                        series = df[('Close', t)]
                    elif (t, 'Close') in df.columns:
                        series = df[(t, 'Close')]
                    else:
                        # fallback: look for a column where one of the levels matches ticker and the other contains 'Close'
                        found = False
                        for col in df.columns:
                            if t in str(col) and 'Close' in str(col):
                                series = df[col]
                                found = True
                                break
                        if not found:
                            # as a last resort, try df['Close'][t] if levels align
                            try:
                                if 'Close' in df.columns.levels[0] and t in df.columns.levels[1]:
                                    series = df['Close'][t]
                            except Exception:
                                series = None
                else:
                    # 単一ティッカーで返ってきた場合や想定外の列構成に対応
                    # Columns might be like 'Close', or 'Close_7203.T', or ('Close',)
                    possible_cols = [c for c in df.columns if t in str(c) or 'Close' in str(c)]
                    if possible_cols:
                        # prefer exact 'Close' column if present
                        if 'Close' in df.columns:
                            series = df['Close']
                        else:
                            series = df[possible_cols[0]]

                if series is None or getattr(series, 'dropna', lambda: series)().empty:
                    if verbose:
                        print(f"{t}: could not locate Close series in batch df")
                    failures.append(t)
                    continue

                close_val = float(series.dropna().iloc[-1])
            except Exception as e:
                if verbose:
                    print(f"{t}: error parsing batch df: {e}")
                failures.append(t)
                close_val = None

            if close_val is not None and close_val <= max_price:
                if verbose:
                    print(f"{t}: price={close_val} <= {max_price} -> added")
                results.append(t)

        # polite pause between batches to avoid throttling
        import time
        time.sleep(sleep_between_batches)

    if verbose:
        print(f"Done. Found {len(results)} tickers; failures: {len(failures)} (sample: {failures[:10]})")

    return results
