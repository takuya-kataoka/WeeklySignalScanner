import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(page_title="株価予想ページ", layout="wide")


@st.cache_data(ttl=3600)
def fetch_history(ticker: str, period: str = "1y", interval: str = "1d"):
    t = yf.Ticker(ticker)
    df = t.history(period=period, interval=interval)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    return df


# (Top explanatory/header removed per user request)


# -----------------------
# 手動予想セクション
# -----------------------
st.markdown('---')
st.header('✍️ 手動で予想を入力・保存する')

PRED_DIR = 'outputs/predictions'
import os
os.makedirs(PRED_DIR, exist_ok=True)

def predictions_path(date=None):
    if date is None:
        date = datetime.now()
    return os.path.join(PRED_DIR, f'predictions_{date.strftime("%Y-%m-%d")}.csv')


with st.expander('手動予想フォーム (開く)', expanded=True):
    col_a, col_b = st.columns(2)
    with col_a:
        manual_ticker = st.text_input('予想する銘柄ティッカー')
        manual_price = st.number_input('予想価格 (数値)', min_value=0.0, value=0.0, format='%f')
    with col_b:
        manual_pct = st.number_input('または変化率（%）で入力', value=0.0, format='%f')
        note = st.text_area('メモ (任意)', value='')

    save_pred = st.button('予想を保存')

    if save_pred:
        # resolve predicted price either from manual_price or pct
        # fetch latest close
        # normalize ticker (allow entering numeric JP codes like 7244)
        def _normalize_ticker(tk: str):
            s = str(tk).strip()
            if s.isdigit():
                return f"{int(s):04d}.T"
            return s

        manual_ticker = _normalize_ticker(manual_ticker)

        df_hist = fetch_history(manual_ticker, period='1mo', interval='1d')
        if df_hist.empty:
            st.error('ティッカーの価格が取得できませんでした。予想を保存できません。')
        else:
            latest_close_local = float(df_hist['Close'].iloc[-1])
            if manual_price and manual_price > 0:
                pred_price = float(manual_price)
            else:
                pred_price = latest_close_local * (1 + float(manual_pct) / 100.0)

            # use created date as target_date (user requested no separate target input)
            target_date = datetime.now().strftime('%Y-%m-%d')

            # save to CSV
            out_path = predictions_path()
            row = {
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'ticker': manual_ticker,
                'target_date': target_date,
                'pred_price': pred_price,
                'note': note
            }
            # append
            import csv
            write_header = not os.path.exists(out_path)
            with open(out_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                if write_header:
                    writer.writeheader()
                writer.writerow(row)

            # Also update a master index of predicted tickers by prediction-date (created date)
            index_path = os.path.join(PRED_DIR, 'predicted_tickers_index.csv')
            idx_row = {'date': datetime.now().strftime('%Y-%m-%d'), 'ticker': manual_ticker}
            write_idx_header = not os.path.exists(index_path)
            # avoid duplicate entries for same date+ticker
            existing = set()
            if os.path.exists(index_path):
                try:
                    import csv as _csv
                    with open(index_path, 'r', encoding='utf-8') as idxf:
                        reader = _csv.DictReader(idxf)
                        for r in reader:
                            existing.add((r.get('date'), r.get('ticker')))
                except Exception:
                    existing = set()

            if (idx_row['date'], idx_row['ticker']) not in existing:
                with open(index_path, 'a', newline='', encoding='utf-8') as idf:
                    writer = csv.DictWriter(idf, fieldnames=list(idx_row.keys()))
                    if write_idx_header:
                        writer.writeheader()
                    writer.writerow(idx_row)

            st.success(f'予想を保存しました: {out_path}')


st.markdown('---')
st.header('🗂 予想履歴と実値比較')

# list prediction files and load
files = sorted([f for f in os.listdir(PRED_DIR) if f.endswith('.csv') and f.startswith('predictions_')], reverse=True)
all_rows = []
for fn in files:
    p = os.path.join(PRED_DIR, fn)
    try:
        d = pd.read_csv(p)
        if not d.empty:
            # remember source file so we can delete individual rows later
            d['source_file'] = fn
            all_rows.append(d)
    except Exception:
        continue

if all_rows:
    preds_df = pd.concat(all_rows, ignore_index=True)
else:
    preds_df = pd.DataFrame()

if preds_df.empty:
    st.info('予想データがありません。まずフォームで予想を保存してください。')
else:
    # Normalize created_at (remove fractional seconds) and group by created_at date (YYYY-MM-DD)
    if 'created_at' in preds_df.columns:
        # normalize various created_at formats (ISO with T, fractional seconds, etc.)
        import re
        def _normalize_created(s):
            if pd.isna(s):
                return s
            t = str(s).strip()
            # replace T with space
            t = t.replace('T', ' ')
            # remove fractional seconds if present
            if '.' in t:
                t = t.split('.')[0]
            return t

        preds_df['created_at'] = preds_df['created_at'].apply(_normalize_created)
        preds_df['created_at'] = pd.to_datetime(preds_df['created_at'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        preds_df['created_at'] = pd.NaT
    preds_df['created_date_key'] = pd.to_datetime(preds_df['created_at'], errors='coerce').dt.strftime('%Y-%m-%d')

    st.write('表示: 全ての保存済み予想')
    # Provide optional grouping by created date, but default to show all
    # expand the most recent date group by default
    date_keys = sorted(preds_df['created_date_key'].dropna().unique(), reverse=True)
    for idx, date_key in enumerate(date_keys):
        group = preds_df[preds_df['created_date_key'] == date_key]
        expanded_default = (idx == 0)
        with st.expander(f"予想日: {date_key} ({len(group)}件)", expanded=expanded_default):
            grp = group.copy()
            # fetch actuals for group's target_date entries
            actuals = []
            charts = []
            for i, r in grp.iterrows():
                tk = r['ticker']
                td = r['target_date']
                actual_price = np.nan
                try:
                    t = yf.Ticker(tk)
                    # be tolerant of timezone/market-date mismatches: fetch a small window around the target date
                    start = (pd.to_datetime(td) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    end = (pd.to_datetime(td) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    h = t.history(start=start, end=end)
                    if not h.empty:
                        h = h.reset_index()
                        # match rows whose date equals target date (YYYY-MM-DD)
                        target_str = pd.to_datetime(td).strftime('%Y-%m-%d')
                        matched = h[h['Date'].dt.strftime('%Y-%m-%d') == target_str]
                        if not matched.empty:
                            actual_price = float(matched['Close'].iloc[0])
                        else:
                            # fallback: take first available close in the window
                            actual_price = float(h['Close'].iloc[0])
                    else:
                        actual_price = np.nan
                except Exception:
                    actual_price = np.nan
                actuals.append(actual_price)

            grp['actual_price'] = actuals

            display_df = grp.sort_values(by='created_at', ascending=False).reset_index(drop=True).copy()
            # 日本語の列名で表示（不要な列は表示しない）
            col_map = {
                'created_at': '作成日時',
                'ticker': '銘柄',
                'target_date': '予測日',
                'pred_price': '予想価格',
                'note': 'メモ',
                'actual_price': '実値'
            }
            display_df = display_df.rename(columns={k: v for k, v in col_map.items() if k in display_df.columns})
            # Compact per-row display: 3 small columns for data, 1 wide for chart
            for i, row in grp.sort_values(by='created_at', ascending=False).reset_index(drop=True).iterrows():
                tk = row['ticker']
                pred_price = row.get('pred_price')
                created = row.get('created_at')
                pred_day = row.get('target_date')
                memo = row.get('note', '')
                actual = row.get('actual_price', np.nan)

                # Left: compact table with borders; Right: chart
                c_left, c4 = st.columns([1, 2])
                # prepare display row with simple formatting
                def _fmt_yen(x):
                    try:
                        return f"{int(float(x))}円"
                    except Exception:
                        return "-"

                df_row = pd.DataFrame([{
                    '作成日時': created,
                    '銘柄': tk,
                    '予測日': pred_day,
                    '予想価格': _fmt_yen(pred_price),
                    '実値': _fmt_yen(actual),
                    'メモ': memo if memo else '-'
                }])

                with c_left:
                    # highlight row if actual reached or exceeded predicted price
                    achieved = False
                    try:
                        if (not pd.isna(actual)) and (pred_price is not None):
                            achieved = float(actual) >= float(pred_price)
                    except Exception:
                        achieved = False

                    try:
                        # use a subtle translucent highlight so it looks good in dark theme
                        styled = df_row.style.apply(lambda s, flag=achieved: ["background-color: rgba(255,255,255,0.03)" if flag else '' for _ in s], axis=1)
                        st.dataframe(styled)
                    except Exception:
                        # fallback
                        st.table(df_row)
                    # deletion button
                    src = row.get('source_file') if 'source_file' in row else None
                    del_key = f"del-{i}-{src}-{tk}-{created}"
                    if st.button('削除', key=del_key):
                        if not src:
                            st.error('元ファイル情報が見つからないため削除できません')
                        else:
                            src_path = os.path.join(PRED_DIR, src)
                            try:
                                df_file = pd.read_csv(src_path)
                                # attempt to match row by created_at, ticker and target_date
                                mask = ~((df_file.get('created_at') == created) & (df_file.get('ticker') == tk) & (df_file.get('target_date') == pred_day))
                                new_df = df_file[mask]
                                if len(new_df) == len(df_file):
                                    # no match found by exact created_at; try matching by ticker+target_date+pred_price
                                    mask2 = ~((df_file.get('ticker') == tk) & (df_file.get('target_date') == pred_day) & (df_file.get('pred_price') == pred_price))
                                    new_df = df_file[mask2]
                                new_df.to_csv(src_path, index=False)
                                # update index file as well
                                index_path = os.path.join(PRED_DIR, 'predicted_tickers_index.csv')
                                if os.path.exists(index_path):
                                    try:
                                        idx_df = pd.read_csv(index_path)
                                        created_date = pd.to_datetime(created, errors='coerce')
                                        if not pd.isna(created_date):
                                            date_str = created_date.strftime('%Y-%m-%d')
                                            idx_df = idx_df[~((idx_df.get('date') == date_str) & (idx_df.get('ticker') == tk))]
                                            idx_df.to_csv(index_path, index=False)
                                    except Exception:
                                        pass
                                st.success('予想を削除しました')
                                st.experimental_rerun()
                            except Exception as e:
                                st.error(f'削除に失敗しました: {e}')
                with c4:
                    # show 1y weekly chart
                    try:
                        hist = yf.Ticker(tk).history(period='1y', interval='1wk')
                        if not hist.empty:
                            fig = go.Figure()
                            fig.add_trace(go.Candlestick(x=hist.index, open=hist['Open'], high=hist['High'], low=hist['Low'], close=hist['Close'], name='価格'))
                            if len(hist) >= 52:
                                fig.add_trace(go.Scatter(x=hist.index, y=hist['Close'].rolling(52).mean(), name='MA52', line=dict(color='orange')))
                            fig.update_layout(title=f"{tk} 週足（1年）", height=300, xaxis_rangeslider_visible=False)
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.write('チャートデータがありません')
                    except Exception:
                        st.write(f"チャートを取得できませんでした: {tk}")

    # cleanup helper column
    preds_df.drop(columns=['created_date_key'], inplace=True, errors='ignore')
