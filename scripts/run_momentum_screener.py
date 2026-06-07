#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import datetime
import traceback
import pandas as pd
import yfinance as yf
import subprocess

# Try to import project helper to load cache; fallback to simple file read
try:
    from data_fetcher import load_ticker_from_cache
except Exception:
    load_ticker_from_cache = None

BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / 'data'
RESULTS_DIR = BASE / 'WeeklySignalScanner-main' / 'outputs' / 'results'
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Build target list from cache
cached = []
if DATA_DIR.exists():
    for p in DATA_DIR.glob('*.parquet'):
        cached.append(p.stem)

if not cached:
    # sample tickers to test
    targets = ['4179.T','8105.T']
else:
    targets = cached

results = []
errors = []

for t in targets:
    try:
        df = None
        if load_ticker_from_cache:
            try:
                df = load_ticker_from_cache(t, cache_dir=str(DATA_DIR))
            except Exception:
                df = None
        if df is None or len(df) < 25:
            mdf = yf.Ticker(t).history(period='40d', interval='1d')
        else:
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            mdf = df.tail(40)
        if mdf is None or mdf.empty or len(mdf) < 25:
            continue
        mdf = mdf.dropna(subset=['Close','Volume'])
        if mdf.empty or len(mdf) < 25:
            continue
        closes = mdf['Close'].astype(float)
        volumes = mdf['Volume'].astype(float)
        today_close = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2])
        price_change_pct = (today_close - prev_close) / prev_close * 100.0
        if len(volumes) >= 22:
            avg_volume_20d = float(volumes.iloc[-22:-2].mean())
        else:
            avg_volume_20d = float(volumes.iloc[:-1].mean()) if len(volumes) > 1 else 0.0
        today_volume = float(volumes.iloc[-1])
        volume_ratio = (today_volume / avg_volume_20d) if avg_volume_20d > 0 else 0.0
        ma25 = closes.rolling(window=25).mean()
        ma25_now = float(ma25.iloc[-1]) if not pd.isna(ma25.iloc[-1]) else None
        deviation_from_ma25 = ((today_close - ma25_now) / ma25_now * 100.0) if ma25_now and ma25_now != 0 else 9999.0
        condition_price = price_change_pct >= 5.0
        condition_volume = volume_ratio >= 3.0
        condition_first_move = deviation_from_ma25 < 20.0
        if condition_price and condition_volume and condition_first_move:
            results.append({
                'コード': t,
                '本日終値': round(today_close,1),
                '前日比(%)': round(price_change_pct,2),
                '出来高倍率': round(volume_ratio,2),
                '25日線乖離率(%)': round(deviation_from_ma25,2)
            })
    except Exception as e:
        errors.append((t, str(e), traceback.format_exc()))

# Save
ts = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')
out = RESULTS_DIR / f"短期_初動_{ts}.csv"
if results:
    pd.DataFrame(results).to_csv(out, index=False, encoding='utf-8-sig')
    print(f"Saved {len(results)} results to {out}")
    # Attempt to add & commit the created result file
    try:
        subprocess.run(['git', '-C', str(BASE), 'config', 'user.name', 'AutoCommit'], check=False)
        subprocess.run(['git', '-C', str(BASE), 'config', 'user.email', 'autocommit@local'], check=False)
        subprocess.run(['git', '-C', str(BASE), 'add', '-f', str(out)], check=False)
        commit_msg = f"chore(screener): add results {out.name}"
        commit_proc = subprocess.run(['git', '-C', str(BASE), 'commit', '-m', commit_msg], capture_output=True, text=True)
        print('git commit returncode=', commit_proc.returncode)
        if commit_proc.stdout:
            print('git stdout:', commit_proc.stdout)
        if commit_proc.stderr:
            print('git stderr:', commit_proc.stderr)
    except Exception as e:
        print('git commit failed:', e)
else:
    print("No results found.")

if errors:
    print(f"Errors for {len(errors)} tickers. Sample:")
    for e in errors[:5]:
        print(e[0], e[1])

print('Done')
