import yfinance as yf
import pandas as pd
from utils import calculate_ma

# EXCLUDED_TICKERS は data_fetcher から取得する（検証CSVで追加されたものを含む）
from data_fetcher import EXCLUDED_TICKERS

def check_signal(ticker, short_window=10, long_window=20, period="2y", interval="1wk", threshold=0.0, data_df=None, require_ma52=True, require_engulfing=True, relaxed_engulfing=False, engulf_within_months=None):
    """
    指定パラメータでティッカーのシグナルを判定する。

    - short_window: 短期移動平均の窓 (例: 10)
    - long_window: 長期移動平均の窓 (例: 20)
    - period: データ取得期間 (yfinance に渡す文字列、例: "2y")
    - interval: データ間隔 (例: "1wk")
    - threshold: 判定の閾値（比率）。最新の短期MAが長期MAより threshold 以上上回る必要がある（例: 0.01 = 1%）
    - require_ma52: 52週MA以上を条件に含めるか
    - require_engulfing: 直近陽線包み足を条件に含めるか
    """
    if short_window >= long_window:
        print(f"{ticker}: short_window({short_window}) must be < long_window({long_window})")
        return False

    if ticker in EXCLUDED_TICKERS:
        print(f"{ticker}: excluded")
        return False

    # If a DataFrame is provided (cache), use it; otherwise fetch from network
    if data_df is None:
        data = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False)

        if data is None or getattr(data, 'empty', True) or len(data) < 2:
            print(f"{ticker}: データが足りません")
            return False
    else:
        # use provided cache (likely daily); resample to weekly (week end = Fri)
        data = data_df.copy()
        try:
            if not isinstance(data.index, pd.DatetimeIndex):
                data.index = pd.to_datetime(data.index)
            # aggregate into weekly candles: first Open, max High, min Low, last Close, sum Volume
            data = data.resample('W-FRI').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        except Exception:
            # if resample fails, fall back to using the cache as-is
            pass

    # remove rows with missing Close (keep DatetimeIndex for period-based checks)
    data.dropna(inplace=True)

    # 直近2足が必要
    if len(data) < 2:
        print(f"{ticker}: データが足りません（2足未満）")
        return False

    # helper: detect bullish engulfing at position i (prev = i-1, curr = i)
    def _is_engulf(prev_row, curr_row):
        try:
            prev_open = float(prev_row["Open"])
            prev_close = float(prev_row["Close"])
            prev_high = float(prev_row.get("High", prev_open))
            prev_low = float(prev_row.get("Low", prev_close))
            curr_open = float(curr_row["Open"])
            curr_close = float(curr_row["Close"])
        except Exception:
            return False
        is_prev_bear = prev_close < prev_open
        is_curr_bull = curr_close > curr_open
        engulfs = (curr_open <= prev_close) and (curr_close >= prev_open)
        wick_engulf = (curr_open <= prev_low) and (curr_close >= prev_high)
        if relaxed_engulfing:
            return is_prev_bear and is_curr_bull and (engulfs or wick_engulf or (curr_close >= prev_open))
        else:
            return is_prev_bear and is_curr_bull and (engulfs or wick_engulf)

    # If engulf_within_months is provided, search weekly and monthly resampled series
    if engulf_within_months is not None and isinstance(engulf_within_months, int) and engulf_within_months > 0:
        # ensure DatetimeIndex for resampling
        if not isinstance(data.index, pd.DatetimeIndex):
            try:
                data.index = pd.to_datetime(data.index)
            except Exception:
                pass

        last_ts = data.index.max()
        start_ts = last_ts - pd.DateOffset(months=engulf_within_months)

        found = False
        # weekly
        try:
            weekly = data.resample('W-FRI').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
            weekly = weekly.loc[weekly.index > start_ts]
            for i in range(1, len(weekly)):
                if _is_engulf(weekly.iloc[i-1], weekly.iloc[i]):
                    found = True
                    break
        except Exception:
            pass

        # monthly
        if not found:
            try:
                monthly = data.resample('M').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
                monthly = monthly.loc[monthly.index > start_ts]
                for i in range(1, len(monthly)):
                    if _is_engulf(monthly.iloc[i-1], monthly.iloc[i]):
                        found = True
                        break
            except Exception:
                pass

        if found:
            # If MA52 required, compute MA52 on weekly data and check latest week
            if require_ma52:
                try:
                    weekly_full = data.resample('W-FRI').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
                    weekly_full['MA52'] = calculate_ma(weekly_full['Close'], 52)
                    ma52_latest = weekly_full['MA52'].iloc[-1]
                    curr_close = weekly_full['Close'].iloc[-1]
                    if pd.isna(ma52_latest) or curr_close < ma52_latest:
                        return False
                except Exception:
                    return False
            print(f"{ticker}: シグナル検出 (陽線包み within {engulf_within_months} months on weekly/monthly)")
            return True

        return False

    # Fallback: original behavior — check latest two candles
    # MA52 を計算（必要な場合のみ）
    ma52_latest = None
    if require_ma52:
        try:
            weekly_full = data.resample('W-FRI').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
            weekly_full['MA52'] = calculate_ma(weekly_full['Close'], 52)
            ma52_latest = weekly_full['MA52'].iloc[-1]
            if pd.isna(ma52_latest):
                print(f"{ticker}: MA52が計算できません（データ不足）")
                return False
        except Exception:
            print(f"{ticker}: MA52計算時にエラー")
            return False

    # 最新と1つ前の足
    prev = data.iloc[-2]
    curr = data.iloc[-1]

    if _is_engulf(prev, curr):
        cond_engulf = True
    else:
        cond_engulf = not require_engulfing

    cond_ma52 = (not require_ma52) or (float(curr['Close']) >= float(ma52_latest))

    if cond_engulf and cond_ma52:
        msg_parts = []
        if require_engulfing:
            msg_parts.append("陽線包み足")
        if require_ma52:
            msg_parts.append(f"MA52以上 (price={float(curr['Close']):.2f} MA52={ma52_latest:.2f})")
        detail = " & ".join(msg_parts) if msg_parts else "条件なし"
        print(f"{ticker}: シグナル検出 ({detail})")
        return True

    return False


def scan_stocks(tickers, short_window=10, long_window=20, period="2y", interval="1wk", threshold=0.0, require_ma52=True, require_engulfing=True):
    """指定したティッカー群を順にチェックし、シグナルが出た銘柄のリストを返す。

    scan_stocks(..., short_window=10, long_window=20, period='2y', interval='1wk', threshold=0.0)
    """
    results = []
    for t in tickers:
        if t in EXCLUDED_TICKERS:
            print(f"{t}: excluded")
            continue
        try:
            ok = check_signal(t, short_window=short_window, long_window=long_window, period=period, interval=interval, threshold=threshold, require_ma52=require_ma52, require_engulfing=require_engulfing)
            if ok:
                results.append(t)
        except Exception as e:
            print(f"{t}: エラー - {e}")
    return results


def scan_stocks_with_cache(tickers, cache_dir='data', short_window=10, long_window=20, period="2y", interval="1wk", threshold=0.0, require_ma52=True, require_engulfing=True, relaxed_engulfing=False, start_date=None, end_date=None):
    """Scan using locally cached per-ticker Parquet files if available.
    If a ticker has no cache file, it will be skipped.
    Optional `start_date` and `end_date` may be provided (strings or date-like);
    when provided the cached DataFrame will be sliced to the specified range
    before being passed to `check_signal`.
    """
    from data_fetcher import load_ticker_from_cache
    results = []
    for t in tickers:
        if t in EXCLUDED_TICKERS:
            print(f"{t}: excluded")
            continue
        try:
            df = load_ticker_from_cache(t, cache_dir=cache_dir)
            if df is None:
                print(f"{t}: cache not found, skipping")
                continue
            # If date range specified, try slicing the cached DataFrame
            try:
                import pandas as _pd
                if start_date or end_date:
                    # ensure DatetimeIndex
                    if not isinstance(df.index, _pd.DatetimeIndex):
                        df.index = _pd.to_datetime(df.index)
                    start_ts = _pd.to_datetime(start_date) if start_date else None
                    end_ts = _pd.to_datetime(end_date) if end_date else None
                    mask = _pd.Series(True, index=df.index)
                    if start_ts is not None:
                        mask &= (df.index >= start_ts)
                    if end_ts is not None:
                        mask &= (df.index <= end_ts)
                    df = df.loc[mask]
                    if df is None or df.empty or len(df) < 2:
                        print(f"{t}: cache has insufficient data after slicing for range {start_date} - {end_date}, skipping")
                        continue
            except Exception:
                # if slicing fails, continue with un-sliced df
                pass
            ok = check_signal(t, short_window=short_window, long_window=long_window, period=period, interval=interval, threshold=threshold, data_df=df, require_ma52=require_ma52, require_engulfing=require_engulfing, relaxed_engulfing=relaxed_engulfing)
            if ok:
                results.append(t)
        except Exception as e:
            print(f"{t}: エラー - {e}")
    return results


def scan_above_ma52_with_cache(tickers, cache_dir='data'):
    """Scan cached parquet files and return tickers whose latest Close >= MA52 (week-based SMA 52)."""
    from data_fetcher import load_ticker_from_cache
    import pandas as pd

    results = []
    for t in tickers:
        if t in EXCLUDED_TICKERS:
            continue
        try:
            df = load_ticker_from_cache(t, cache_dir=cache_dir)
            if df is None:
                # no cache
                continue
            df = df.copy()
            df.dropna(subset=['Close'], inplace=True)
            df.reset_index(drop=True, inplace=True)
            if len(df) < 52:
                continue
            ma52 = df['Close'].rolling(window=52).mean().iloc[-1]
            last = df['Close'].iloc[-1]
            if pd.isna(ma52):
                continue
            if float(last) >= float(ma52):
                results.append(t)
        except Exception:
            # ignore per-ticker errors
            continue
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
    all_codes = [f"{i:04d}.T" for i in range(start, end + 1) if f"{i:04d}.T" not in EXCLUDED_TICKERS]

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
