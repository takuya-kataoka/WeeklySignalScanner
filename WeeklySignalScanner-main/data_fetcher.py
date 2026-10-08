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


_OHLCV = ['Open', 'High', 'Low', 'Close', 'Volume']
_UNIVERSE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'universe_jp.txt')


def load_universe(path=None):
    """実在する日本株ティッカー一覧（例: '7203.T'）を返す。無ければ空リスト。

    9000 コードの総当たりは実在しないコードへのリクエストが半数以上を占めるため、
    同梱の universe_jp.txt を取得対象にする。
    """
    path = path or _UNIVERSE_FILE
    try:
        with open(path, encoding='utf-8') as f:
            return [ln.strip() for ln in f if ln.strip()]
    except OSError:
        return []


def _strip_tz(df):
    idx = pd.to_datetime(df.index)
    if getattr(idx, 'tz', None) is not None:
        idx = idx.tz_localize(None)
    df = df.copy()
    df.index = idx
    return df


def _extract(df, ticker, single):
    """yf.download の結果から 1 銘柄分の OHLCV を取り出す。取れなければ None。"""
    if df is None or getattr(df, 'empty', True):
        return None
    if isinstance(df.columns, pd.MultiIndex):
        if ticker in df.columns.get_level_values(0):
            sub = df[ticker]
        elif ticker in df.columns.get_level_values(1):
            sub = df.xs(ticker, level=1, axis=1)
        else:
            return None
    elif single:
        sub = df
    else:
        return None
    cols = [c for c in _OHLCV if c in sub.columns]
    if 'Close' not in cols:
        return None
    sub = sub[cols].dropna(subset=['Close'])
    return None if sub.empty else _strip_tz(sub)


def _last_expected_trading_day():
    """直近の営業日（土日のみ考慮、祝日は考慮しない）。"""
    today = pd.Timestamp.today().normalize()
    return today if today.weekday() < 5 else today - pd.offsets.BDay(1)


def _download(batch, retry_count, sleep, verbose, **kwargs):
    attempt = 0
    while attempt <= retry_count:
        try:
            return yf.download(batch, progress=False, group_by='ticker', auto_adjust=False, **kwargs)
        except Exception as e:
            attempt += 1
            if attempt > retry_count:
                if verbose:
                    print(f"batch download failed after {attempt} attempts: {e}")
                return None
            wait = sleep * (2 ** (attempt - 1))
            if verbose:
                print(f"batch download error, retrying after {wait}s: {e}")
            time.sleep(wait)


def _save(ticker, new, out_dir, existing, verbose):
    """new を保存する。existing があれば結合（重複日は new を優先）。"""
    if existing is not None:
        new = pd.concat([existing, new[[c for c in existing.columns if c in new.columns]]])
        new = new[~new.index.duplicated(keep='last')].sort_index()
    path = os.path.join(out_dir, f"{ticker}.parquet")
    new.to_parquet(path)
    if verbose:
        print(f"Saved {ticker} -> {path}")


def _fetch_group(codes, existing, out_dir, batch_size, retry_count, sleep, verbose, **dl_kwargs):
    total_batches = (len(codes) - 1) // batch_size + 1 if codes else 0
    for batch_idx, i in enumerate(range(0, len(codes), batch_size), start=1):
        batch = codes[i:i + batch_size]
        if verbose:
            print(f"Fetching batch {batch_idx}/{total_batches} (size={len(batch)})")
        df = _download(batch, retry_count, sleep, verbose, **dl_kwargs)
        if df is None or getattr(df, 'empty', True):
            # 銘柄ごとの再取得はしない（存在しないコードで 1 件ずつ待つのが遅さの主因だった）
            if verbose:
                print("Batch returned no data — skipped")
        else:
            for t in batch:
                try:
                    sub = _extract(df, t, single=len(batch) == 1)
                    if sub is None:
                        if verbose:
                            print(f"{t}: no valid data, skipping save")
                        continue
                    _save(t, sub, out_dir, existing.get(t), verbose)
                except Exception as e:
                    if verbose:
                        print(f"{t}: error saving - {e}")
        if batch_idx < total_batches:
            time.sleep(sleep)


def fetch_and_save_list(tickers, batch_size=200, period='6mo', interval='1d', out_dir=None, retry_count=2, sleep_between_batches=1.0, allow_excluded=False, verbose=False, incremental=True):
    """
    指定されたティッカー一覧をバッチで取得して Parquet に保存します。
    `tickers` は ['7201.T', '7202.T', ...] の形式のリストを想定します。

    incremental=True（日足のみ）: 保存済みの銘柄は最終日の少し前から差分だけ取得して結合し、
    すでに直近営業日まである銘柄はスキップします。未保存の銘柄は `period` 分を取得します。
    """
    if out_dir is None:
        out_dir = config.DATA_DIR
    _ensure_dir(out_dir)

    codes = list(tickers) if allow_excluded else [t for t in tickers if t not in EXCLUDED_TICKERS]
    if not codes:
        if verbose:
            print('No tickers to fetch')
        return

    fresh, stale, existing = [], [], {}
    if incremental and interval == '1d':
        expected = _last_expected_trading_day()
        for t in codes:
            old = load_ticker_from_cache(t, cache_dir=out_dir)
            if old is None or getattr(old, 'empty', True):
                fresh.append(t)
                continue
            try:
                old = _strip_tz(old)
                last = old.index.max().normalize()
            except Exception:
                fresh.append(t)
                continue
            if last >= expected:
                continue  # 最新
            existing[t] = old
            stale.append((t, last))
        if verbose:
            print(f"new={len(fresh)} update={len(stale)} up-to-date={len(codes) - len(fresh) - len(stale)}")
    else:
        fresh = codes

    if stale:
        start = (min(last for _, last in stale) - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
        _fetch_group([t for t, _ in stale], existing, out_dir, batch_size, retry_count, sleep_between_batches, verbose, start=start, interval=interval)
    if fresh:
        _fetch_group(fresh, {}, out_dir, batch_size, retry_count, sleep_between_batches, verbose, period=period, interval=interval)


def fetch_and_save_tickers(start=1000, end=9999, batch_size=200, period='6mo', interval='1d', out_dir=None, retry_count=2, sleep_between_batches=1.0, allow_excluded=False, verbose=False, use_universe=False, incremental=True):
    """
    指定範囲のティッカー（4桁コードに .T を付与）をバッチで取得して保存します。
    use_universe=True なら総当たりせず universe_jp.txt の銘柄のうち範囲内のものだけ取得します。
    """
    if use_universe:
        codes = [t for t in load_universe() if start <= int(t[:4]) <= end]
    else:
        codes = [f"{i:04d}.T" for i in range(start, end + 1)]
    fetch_and_save_list(codes, batch_size=batch_size, period=period, interval=interval, out_dir=out_dir, retry_count=retry_count, sleep_between_batches=sleep_between_batches, allow_excluded=allow_excluded, verbose=verbose, incremental=incremental)


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
