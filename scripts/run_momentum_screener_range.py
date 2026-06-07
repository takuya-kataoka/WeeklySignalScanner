#!/usr/bin/env python3
import os
from pathlib import Path
import datetime
import traceback
import pandas as pd
import yfinance as yf
import subprocess

# params
START = os.environ.get('SCR_START', '2026-05-01')
END = os.environ.get('SCR_END', '2026-05-31')

BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / 'data'
RESULTS_DIR = BASE / 'WeeklySignalScanner-main' / 'outputs' / 'results'
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# build targets from cache if available, else a small sample
cached = []
if DATA_DIR.exists():
    for p in DATA_DIR.glob('*.parquet'):
        cached.append(p.stem)

if not cached:
    targets = ['4179.T','8105.T']
else:
    targets = cached

results = []
errors = []

# use end inclusive: yf.history end is exclusive, so add one day
end_dt = pd.to_datetime(END) + pd.Timedelta(days=1)
for t in targets:
    try:
        # fetch daily history for the date range
        df = yf.Ticker(t).history(start=START, end=end_dt.strftime('%Y-%m-%d'), interval='1d')
        if df is None or df.empty:
            continue
        df = df.dropna(subset=['Close','Volume'])
        if df.empty:
            continue
        # need at least 2 rows to compare last vs prev
        if len(df) < 2:
            continue
        # use last row as 'target day'
        today_close = float(df['Close'].iloc[-1])
        prev_close = float(df['Close'].iloc[-2])
        price_change_pct = (today_close - prev_close) / prev_close * 100.0
        # compute avg volume of previous up to 20 trading days excluding the target day
        volumes = df['Volume'].astype(float)
        if len(volumes) >= 21:
            avg_vol_20 = float(volumes.iloc[-21:-1].mean())
        else:
            avg_vol_20 = float(volumes.iloc[:-1].mean()) if len(volumes) > 1 else 0.0
        today_vol = float(volumes.iloc[-1])
        volume_ratio = (today_vol / avg_vol_20) if avg_vol_20 > 0 else 0.0
        # MA25 computed over available data (prefer prior 25 days ending at target day)
        closes = df['Close'].astype(float)
        ma25 = closes.rolling(window=25).mean()
        ma25_now = float(ma25.iloc[-1]) if not pd.isna(ma25.iloc[-1]) else None
        deviation_from_ma25 = ((today_close - ma25_now) / ma25_now * 100.0) if ma25_now and ma25_now != 0 else 9999.0
        # conditions
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

# Save results
ts = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')
out = RESULTS_DIR / f"短期_初動_{START}_to_{END}_{ts}.csv"
if results:
    pd.DataFrame(results).to_csv(out, index=False, encoding='utf-8-sig')
    print(f"Saved {len(results)} results to {out}")
    # add & commit
    try:
        subprocess.run(['git', '-C', str(BASE), 'config', 'user.name', 'AutoCommit'], check=False)
        subprocess.run(['git', '-C', str(BASE), 'config', 'user.email', 'autocommit@local'], check=False)
        subprocess.run(['git', '-C', str(BASE), 'add', '-f', str(out)], check=False)
        commit_msg = f"chore(screener-range): add results {out.name}"
        commit_proc = subprocess.run(['git', '-C', str(BASE), 'commit', '-m', commit_msg], capture_output=True, text=True)
        print('git commit returncode=', commit_proc.returncode)
    except Exception as e:
        print('git commit failed:', e)
else:
    print("No results found for the given date range.")

if errors:
    print(f"Errors for {len(errors)} tickers. Sample:")
    for e in errors[:5]:
        print(e[0], e[1])

print('Done')
