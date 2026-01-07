import os
import glob
from datetime import datetime

import pandas as pd
import yfinance as yf
import streamlit as st


def find_latest_csv():
    # 探索パターンを拡張: 英語名／日本語名のどちらでも拾う
    patterns = [
        "outputs/results/jp_all_ma52_engulfing_*.csv",
        "outputs/results/*MA52*陽線**.csv",
        "outputs/results/*全銘柄*MA52*陽線**.csv",
        "outputs/results/*with_prices*.csv",
    ]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    files = list(set(files))
    if not files:
        # fallback to any csv in outputs/results
        all_csv = glob.glob('outputs/results/*.csv')
        if all_csv:
            return max(all_csv, key=os.path.getmtime)
        raise FileNotFoundError("No results CSV found in outputs/results/")
    return max(files, key=os.path.getmtime)


def get_ticker_column(df: pd.DataFrame) -> str:
    candidates = ["ticker", "Ticker", "symbol", "Symbol", "code", "Code"]
    for c in candidates:
        if c in df.columns:
            return c
    return df.columns[0]


@st.cache_data(ttl=60 * 30)
def fetch_current_price(ticker: str):
    try:
        # yfinance accepts tickers like '1328.T'
        t = yf.Ticker(ticker)
        # try fast history call
        hist = t.history(period="1d", interval="1d")
        if not hist.empty:
            return float(hist['Close'].iloc[-1])
        info = t.info
        return info.get('regularMarketPrice') or info.get('previousClose')
    except Exception:
        return None


def main():
    st.set_page_config(page_title="JP MA52 Engulfing - Sorted by Price", layout="wide")
    st.title("抽出銘柄 — 現在価格でソートして表示")

    csv_path = find_latest_csv()
    st.caption(f"読み込みファイル: `{csv_path}`")

    df = pd.read_csv(csv_path)
    ticker_col = get_ticker_column(df)
    df = df.copy()
    # ensure tickers are strings
    df[ticker_col] = df[ticker_col].astype(str)

    st.markdown("---")
    col1, col2 = st.columns([1, 3])
    with col1:
        asc = st.radio("並び順", ("昇順（安い→高い）", "降順（高い→安い）"), index=0)
        compute = st.button("現在価格でソートして表示 / CSV保存")
    with col2:
        st.write("銘柄数: ", len(df))

    if compute:
        prices = []
        progress = st.progress(0)
        status = st.empty()
        for i, tk in enumerate(df[ticker_col].tolist()):
            status.text(f"取得中: {tk} ({i+1}/{len(df)})")
            p = fetch_current_price(tk)
            prices.append(p)
            progress.progress(int((i + 1) / len(df) * 100))

        df['current_price'] = prices
        ascending = True if asc.startswith("昇順") else False
        df_sorted = df.sort_values(by='current_price', ascending=ascending, na_position='last')

        st.markdown("### 結果（現在価格を取得してソート）")
        st.dataframe(df_sorted.reset_index(drop=True))

        # Save sorted CSV
        date = datetime.now().strftime('%Y-%m-%d')
        out_dir = 'outputs/results'
        os.makedirs(out_dir, exist_ok=True)
        out_name = f'jp_all_ma52_engulfing_sorted_{date}.csv'
        out_path = os.path.join(out_dir, out_name)
        df_sorted.to_csv(out_path, index=False)
        st.success(f"ソート済みCSVを保存しました: `{out_path}`")

        # Download button
        csv_bytes = df_sorted.to_csv(index=False).encode('utf-8')
        st.download_button("CSVをダウンロード", csv_bytes, file_name=out_name, mime='text/csv')

    else:
        st.info("「現在価格でソートして表示 / CSV保存」ボタンを押すと価格取得してソートします。")


if __name__ == '__main__':
    main()
