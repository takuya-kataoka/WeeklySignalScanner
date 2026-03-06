#!/usr/bin/env python3
"""
Improved monthly scan runner:
- Scans all Japanese tickers (1000-9999)
- Uses cached parquet files in data/ when present
- If missing, attempts to fetch monthly data via yfinance with retries and caches parquet
- Detects bullish engulfing within `months_within` months
- Computes max rise over `lookahead_months` after signal
- Applies rise filter (max_allowed_rise_pct)
- Appends results incrementally to CSV and logs failures incrementally
- Writes a final report file when complete
"""
import sys
from pathlib import Path
import csv
import datetime
import time
import traceback

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import yfinance as yf
import pandas as pd
import config
import scan_monthly_engulfing_jp as sm

DATA_DIR = repo_root / 'data'
RESULTS_DIR = repo_root / 'outputs' / 'results'
LOGS_DIR = repo_root / 'outputs' / 'logs'
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Parameters
months_within = 6
lookahead_months = 6
max_allowed_rise_pct = 100.0
rise_filter_enable = True
retries = 3
sleep_between_retries = 2.0
min_allowed_rise_pct = 0.0

# Files
ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
out_csv = RESULTS_DIR / f"月足_陽線包み_within{months_within}m_improved_{int(max_allowed_rise_pct)}pct_{ts}.csv"
fail_log = RESULTS_DIR / f"monthly_scan_failures_{ts}.log"
report_file = RESULTS_DIR / f"monthly_scan_report_{ts}.txt"

# Build ticker list (full range)
all_tickers = sm.get_japanese_tickers(1000, 9999)

total = len(all_tickers)
matched = 0
cached_used = 0
fetched_cached = 0
missing_data_count = 0
failures = []

# Prepare CSV header
fieldnames = ['ticker', 'pattern', 'months_ago', 'latest_price', 'prev_open', 'prev_close', 'curr_open', 'curr_close', 'max_rise_pct']
if not out_csv.exists():
    with open(out_csv, 'w', newline='', encoding='utf-8') as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()

start_time = time.time()

for idx, ticker in enumerate(all_tickers, 1):
    try:
        if idx % 200 == 0:
            elapsed = time.time() - start_time
            print(f"{datetime.datetime.utcnow().isoformat()} Progress: {idx}/{total} elapsed={int(elapsed)}s matched={matched} failures={len(failures)}")

        # Try load cache
        parquet_path = DATA_DIR / f"{ticker}.parquet"
        dfm = None
        if parquet_path.exists():
            try:
                dfm = pd.read_parquet(parquet_path)
                cached_used += 1
            except Exception as e:
                # corrupted cache -- remove and refetch
                try:
                    parquet_path.unlink()
                except Exception:
                    pass
                dfm = None

        if dfm is None:
            # fetch with retries
            for attempt in range(1, retries + 1):
                try:
                    yfobj = yf.Ticker(ticker)
                    dfm = yfobj.history(period='3y', interval='1mo')
                    if dfm is None or dfm.empty or len(dfm) < 2:
                        raise ValueError('no_monthly_data')
                    # cache parquet
                    try:
                        dfm.to_parquet(parquet_path)
                        fetched_cached += 1
                    except Exception:
                        pass
                    break
                except Exception as e:
                    if attempt < retries:
                        time.sleep(sleep_between_retries * attempt)
                        continue
                    else:
                        failures.append((ticker, str(e)))
                        missing_data_count += 1
                        with open(fail_log, 'a', encoding='utf-8') as lf:
                            lf.write(f"{ticker},fetch_failed,{str(e)}\n")
                        dfm = None
        # If still no data, skip
        if dfm is None or dfm.empty or len(dfm) < 2:
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
            prev_high = float(prev['High']) if 'High' in prev else max(prev_open, prev_close)
            prev_low = float(prev['Low']) if 'Low' in prev else min(prev_open, prev_close)
            curr_open = float(curr['Open'])
            curr_close = float(curr['Close'])

            is_prev_bearish = prev_close < prev_open
            is_curr_bullish = curr_close > curr_open
            bullish_engulfs = (curr_open <= prev_close) and (curr_close >= prev_open)
            wick_engulf = (curr_open <= prev_low) and (curr_close >= prev_high)

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

                if rise_filter_enable and (max_rise_pct > float(max_allowed_rise_pct) or max_rise_pct < float(min_allowed_rise_pct)):
                    # filtered out
                    found = False
                else:
                    row = {
                        'ticker': ticker,
                        'pattern': 'bullish_engulfing',
                        'months_ago': k,
                        'latest_price': curr_close,
                        'prev_open': prev_open,
                        'prev_close': prev_close,
                        'curr_open': curr_open,
                        'curr_close': curr_close,
                        'max_rise_pct': max_rise_pct,
                    }
                    # append to CSV
                    with open(out_csv, 'a', newline='', encoding='utf-8') as cf:
                        writer = csv.DictWriter(cf, fieldnames=fieldnames)
                        writer.writerow(row)
                    matched += 1
                    found = True
                    break
        # end for k
    except Exception as e:
        failures.append((ticker, str(e)))
        with open(fail_log, 'a', encoding='utf-8') as lf:
            lf.write(f"{ticker},exception,{traceback.format_exc()}\n")

# end for all_tickers

end_time = time.time()
summary = {
    'total_tickers': total,
    'matched': matched,
    'cached_used': cached_used,
    'fetched_cached': fetched_cached,
    'missing_data_count': missing_data_count,
    'failures_count': len(failures),
    'duration_sec': int(end_time - start_time),
}

with open(report_file, 'w', encoding='utf-8') as rf:
    rf.write('Monthly Scan Improved Report\n')
    rf.write('Timestamp: ' + datetime.datetime.utcnow().isoformat() + '\n')
    for k, v in summary.items():
        rf.write(f"{k}: {v}\n")
    rf.write('\nSample failures (up to 50):\n')
    for f in failures[:50]:
        rf.write(f"{f[0]}: {f[1]}\n")

print('Done. Report:', report_file)
print('Results CSV:', out_csv)
print('Failures log:', fail_log)
