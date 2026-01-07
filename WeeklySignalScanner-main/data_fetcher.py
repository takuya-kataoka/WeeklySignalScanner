import os
import time
import yfinance as yf
import pandas as pd
import config

# 今後取得しない除外銘柄
EXCLUDED_TICKERS = {f"{code}.T" for code in [1326, 1543, 1555, 1586, 1593, 1618, 1621, 1672, 1674, 1679, 1736, 1795, 1807, 2012, 2013, 1325, 2050, 2250, 1656,2504,4230,4186,4315,4276,4188,4145,4277,4154,4305,4156,4254,4200,4126,4139,4162,4168,4285,4274,4255,4283,4134,4144,4193,4295,4306,4147,4244,4259,4239,4297,4271,4235,4278,4164,4310,4214,4153,4298,4318,4209,4273,4181,4194,4177,4284,4128,4163,4261,4180,4287,4257,4191,4159,4228,4221,4146,4218,4178,4308,4155,4260,4167,4203,4151,4216,4229,4299,4251,4237,4137,4311,4264,4253,4182,4124,4252,4136,4266,4289,4294,4222,4173,4301,4309,4169,4302,4281,4282,4231,4210,4223,4280,4304,4233,4211,4129,4232,4243,4267,4196,4135,4190,4143,4250,4286,4142,4249,4303,4204,4184,4120,4131,4165,4202,4122,4300,4217,4296,4130,4205,4213,4160,4185,4176,4138,4291,4246,4293,4238,4215,4248,4127,4219,4174,4226,4157,4272,4234,4242,4307,4150,4269,4179,4312,4207,4121,4292,4270,4171,4201,4290,4119,4236,4198,4152,4288,4212,4245,4247,4148,4161,4279,4195,4268,4241,4227,4175,4140,4183,4314,4189,4158,4197,4240,4123
]}

# 追加: 検証済みで存在しないと判定された銘柄があれば、outputs の CSV から読み込んで EXCLUDED_TICKERS に加える
try:
    import csv
    _verified = os.path.join(os.path.dirname(__file__), 'outputs', 'verified_failed_tickers_2025-12-26.csv')
    if os.path.exists(_verified):
        try:
            with open(_verified, newline='') as _f:
                rdr = csv.DictReader(_f)
                for r in rdr:
                    if r.get('exists', '').strip().lower() == 'no':
                        t = r.get('ticker', '').strip()
                        if t:
                            EXCLUDED_TICKERS.add(t)
        except Exception:
            # 不整合があってもフェールしない
            pass
except Exception:
    pass


def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def fetch_and_save_tickers(start=1000, end=9999, batch_size=200, period='6mo', interval='1d', out_dir=None, retry_count=2, sleep_between_batches=1.0, allow_excluded=False, verbose=False):
    """
    指定範囲のティッカー（4桁コードに .T を付与）をバッチで取得して、各ティッカーごとに Parquet ファイルとして保存します。

    デフォルトで半年分（period='6mo'）を取得します。
    """
    if out_dir is None:
        out_dir = config.DATA_DIR
    _ensure_dir(out_dir)

    # build list of codes; optionally respect EXCLUDED_TICKERS
    all_codes = [f"{i:04d}.T" for i in range(start, end + 1)]
    if not allow_excluded:
        all_codes = [c for c in all_codes if c not in EXCLUDED_TICKERS]
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


def load_ticker_from_cache(ticker, cache_dir=None):
    if cache_dir is None:
        cache_dir = config.DATA_DIR
    path = os.path.join(cache_dir, f"{ticker}.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        return df
    except Exception:
        return None


def fetch_and_save_list(tickers, batch_size=200, period='6mo', interval='1d', out_dir=None, retry_count=2, sleep_between_batches=1.0, allow_excluded=False, verbose=False):
    """
    指定されたティッカー一覧をバッチで取得して Parquet に保存します。
    `tickers` は ['7201.T', '7202.T', ...] の形式のリストを想定します。
    """
    if out_dir is None:
        out_dir = config.DATA_DIR
    _ensure_dir(out_dir)

    # filter excluded unless allow_excluded is set
    if allow_excluded:
        all_codes = list(tickers)
    else:
        all_codes = [t for t in tickers if t not in EXCLUDED_TICKERS]
    total = len(all_codes)
    if total == 0:
        if verbose:
            print('No tickers to fetch')
        return

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
                    if ('Close', t) in df.columns:
                        series_df = df.xs(t, level=1, axis=1)
                    elif (t, 'Close') in df.columns:
                        series_df = df.xs(t, level=0, axis=1)
                    else:
                        cols = [c for c in df.columns if t in str(c)]
                        if cols:
                            series_df = df[cols]
                else:
                    possible_cols = [c for c in df.columns if t in str(c)]
                    if possible_cols:
                        series_df = df[possible_cols]

                if series_df is None or getattr(series_df, 'empty', True):
                    single = yf.Ticker(t).history(period=period, interval=interval)
                    if single is None or getattr(single, 'empty', True):
                        if verbose:
                            print(f"{t}: no data (batch+single)")
                        continue
                    series_df = single

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
