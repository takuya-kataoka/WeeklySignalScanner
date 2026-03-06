#!/usr/bin/env python3
"""
Run monthly engulfing scan using cached data when available.
Parameters hard-coded for quick repro: months_within=6, lookahead_months=6, max_rise_pct=100
"""
import sys
from pathlib import Path
import csv
import datetime

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import yfinance as yf
import config
import scan_monthly_engulfing_jp as sm

DATA_DIR = repo_root / 'data'
RESULTS_DIR = repo_root / 'outputs' / 'results'
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

months_within = 6
lookahead_months = 6
max_allowed_rise_pct = 100.0
rise_filter_enable = True
cache_only = True

# Build ticker list
cached_files = sorted([p.stem for p in DATA_DIR.glob('*.parquet')]) if DATA_DIR.exists() else []
if cache_only and cached_files:
    tickers = cached_files
else:
    tickers = sm.get_japanese_tickers(1000, 9999)

print(f"Tickers to scan: {len(tickers)} (cache_only={cache_only}, cached_count={len(cached_files)})")

results = []
missing_count = 0
fetch_failures = []

for i, t in enumerate(tickers, 1):
    if i % 200 == 0:
        print(f"Progress: {i}/{len(tickers)}")
    try:
        dfm = yf.Ticker(t).history(period='3y', interval='1mo')
        if dfm is None or dfm.empty or len(dfm) < 2:
            missing_count += 1
            fetch_failures.append((t, 'data_missing'))
            continue
        L = len(dfm)
        found = False
        for k in range(1, months_within + 1):
            if L - (k + 1) < 0:
                break
            prev = dfm.iloc[-(k+1)]
            curr = dfm.iloc[-k]
            prev_open = float(prev['Open'])
            prev_close = float(prev['Close'])
            curr_open = float(curr['Open'])
            curr_close = float(curr['Close'])
            is_prev_bearish = prev_close < prev_open
            is_curr_bullish = curr_close > curr_open
            bullish_engulfs = (curr_open <= prev_close) and (curr_close >= prev_open)
            wick_engulf = (curr_open <= prev_low) and (curr_close >= prev_high) if False else (curr_open <= prev_open and curr_close >= prev_close)
            if is_prev_bearish and is_curr_bullish and (bullish_engulfs or wick_engulf):
                signal_idx = L - k
                end_idx = min(signal_idx + lookahead_months + 1, L)
                try:
                    closes = dfm['Close'].iloc[signal_idx:end_idx].astype(float)
                    max_close = float(closes.max()) if not closes.empty else float(curr_close)
                except Exception:
                    max_close = float(curr_close)
                try:
                    max_rise_pct = round((max_close - float(curr_close)) / float(curr_close) * 100.0, 2)
                except Exception:
                    max_rise_pct = 0.0
                if rise_filter_enable and (max_rise_pct > float(max_allowed_rise_pct)):
                    # skip
                    found = False
                else:
                    results.append({
                        'ticker': t,
                        'pattern': 'bullish_engulfing',
                        'months_ago': k,
                        'latest_price': curr_close,
                        'prev_open': prev_open,
                        'prev_close': prev_close,
                        'curr_open': curr_open,
                        'curr_close': curr_close,
                        'max_rise_pct': max_rise_pct,
                    })
                    found = True
                    break
        if not found:
            # not matching or filtered out
            pass
    except Exception as e:
        fetch_failures.append((t, str(e)))

# Write CSV with timestamp
ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
out_path = RESULTS_DIR / f"月足_陽線包み_within{months_within}m_filtered_{int(max_allowed_rise_pct)}pct_{ts}.csv"
with open(out_path, 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['ticker', 'pattern', 'months_ago', 'latest_price', 'prev_open', 'prev_close', 'curr_open', 'curr_close', 'max_rise_pct']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print(f"Saved {len(results)} results to {out_path}")
print(f"Missing data count: {missing_count}, fetch_failures: {len(fetch_failures)}")
if fetch_failures:
    logp = RESULTS_DIR / f"monthly_scan_failures_{ts}.log"
    with open(logp, 'w', encoding='utf-8') as lf:
        for t, msg in fetch_failures:
            lf.write(f"{t},{msg}\n")
    print(f"Wrote failures to {logp}")
