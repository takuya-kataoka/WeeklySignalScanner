#!/usr/bin/env python3
import os
from pathlib import Path
import datetime
import pandas as pd
import csv
import subprocess

START = '2026-05-01'
END = '2026-05-31'

BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / 'data'
RESULTS_DIR = BASE / 'WeeklySignalScanner-main' / 'outputs' / 'results'
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

results = []
errors = []

# iterate parquet files in cache
parquets = sorted(DATA_DIR.glob('*.parquet'))
for p in parquets:
    t = p.stem
    try:
        df = pd.read_parquet(p)
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        # filter rows in range
        mask = (df.index >= pd.to_datetime(START)) & (df.index <= pd.to_datetime(END))
        period_df = df.loc[mask]
        if period_df.empty:
            continue
        # iterate each candidate day in period
        for i, dt in enumerate(period_df.index):
            # find position in full df
            pos = df.index.get_loc(dt)
            if pos == 0:
                continue
            # require at least previous 1 day and prior 20 days for avg (excluding target)
            start_idx = max(0, pos - 22)
            prev_window = df.iloc[start_idx:pos]  # excludes today
            if prev_window.empty:
                continue
            # compute metrics
            today = df.iloc[pos]
            prev = df.iloc[pos - 1]
            try:
                today_close = float(today['Close'])
                prev_close = float(prev['Close'])
            except Exception:
                continue
            price_change_pct = (today_close - prev_close) / prev_close * 100.0 if prev_close != 0 else 0.0
            vols = prev_window['Volume'].astype(float)
            if len(vols) >= 1:
                avg_vol_20 = float(vols.tail(20).mean()) if len(vols) >= 20 else float(vols.mean())
            else:
                avg_vol_20 = 0.0
            today_vol = float(today['Volume']) if 'Volume' in today.index else 0.0
            volume_ratio = (today_vol / avg_vol_20) if avg_vol_20 > 0 else 0.0
            closes = df['Close'].astype(float)
            ma25 = closes.rolling(window=25).mean()
            ma25_now = float(ma25.iloc[pos]) if not pd.isna(ma25.iloc[pos]) else None
            deviation_from_ma25 = ((today_close - ma25_now) / ma25_now * 100.0) if ma25_now and ma25_now != 0 else 9999.0
            # conditions
            if price_change_pct >= 5.0 and volume_ratio >= 3.0 and deviation_from_ma25 < 20.0:
                results.append({'コード': t, '検出日': dt.strftime('%Y-%m-%d'), '本日終値': round(today_close,1), '前日比(%)': round(price_change_pct,2), '出来高倍率': round(volume_ratio,2), '25日線乖離率(%)': round(deviation_from_ma25,2)})
    except Exception as e:
        errors.append((t, str(e)))

# save
ts = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')
out = RESULTS_DIR / f"短期_初動_cache_{START}_to_{END}_{ts}.csv"
if results:
    with open(out, 'w', newline='', encoding='utf-8-sig') as fh:
        writer = csv.DictWriter(fh, fieldnames=['コード','検出日','本日終値','前日比(%)','出来高倍率','25日線乖離率(%)'])
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved {len(results)} results to {out}")
    # add & commit
    try:
        subprocess.run(['git', '-C', str(BASE), 'config', 'user.name', 'AutoCommit'], check=False)
        subprocess.run(['git', '-C', str(BASE), 'config', 'user.email', 'autocommit@local'], check=False)
        subprocess.run(['git', '-C', str(BASE), 'add', '-f', str(out)], check=False)
        commit_msg = f"chore(screener-cache): add results {out.name}"
        commit_proc = subprocess.run(['git', '-C', str(BASE), 'commit', '-m', commit_msg], capture_output=True, text=True)
        print('git commit returncode=', commit_proc.returncode)
    except Exception as e:
        print('git commit failed:', e)
else:
    print('No results found in cache for given date range.')

if errors:
    print(f"Errors for {len(errors)} tickers. Sample:")
    for e in errors[:5]:
        print(e)

print('Done')
