import os
import glob
from datetime import datetime

import pandas as pd
import streamlit as st


def list_result_csvs():
    files = sorted(glob.glob('outputs/results/*.csv'), key=os.path.getmtime, reverse=True)
    return files


def get_ticker_column(df: pd.DataFrame) -> str:
    candidates = ["ticker", "Ticker", "symbol", "Symbol", "code", "Code"]
    for c in candidates:
        if c in df.columns:
            return c
    return df.columns[0]


def fetch_price_from_cache(ticker: str):
    # Try local parquet under data/<ticker>.parquet and return last Close-like column
    path = os.path.join('data', f"{ticker}.parquet")
    if not os.path.exists(path):
        return None
    try:
        d = pd.read_parquet(path)
        for col in ('Close', 'close', 'Close/Last'):
            if col in d.columns:
                s = d[col].dropna()
                if len(s):
                    return float(s.iloc[-1])
    except Exception:
        return None
    return None


def main():
    st.set_page_config(page_title="抽出銘柄一覧 (results)", layout="wide")
    st.title("抽出銘柄一覧 — outputs/results")

    files = list_result_csvs()
    if not files:
        st.error('outputs/results に CSV ファイルが見つかりません')
        return

    choice = st.sidebar.selectbox('表示するファイルを選択', files, format_func=lambda p: os.path.basename(p))
    st.sidebar.write(f'最終更新: {datetime.fromtimestamp(os.path.getmtime(choice))}')

    df = pd.read_csv(choice)
    st.sidebar.write(f'行数: {len(df)}')

    ticker_col = get_ticker_column(df)
    st.write(f"読み込みファイル: `{choice}` — 銘柄列: `{ticker_col}`")

    # If current_price exists, allow quick sort; otherwise provide cache-based computation
    if 'current_price' in df.columns:
        st.info('このファイルには既に `current_price` 列があります。')
        cols = df.columns.tolist()
        sort_by = st.sidebar.selectbox('並べ替え列', cols, index=cols.index('current_price'))
        asc = st.sidebar.checkbox('昇順 (小→大)', value=True)
        df_sorted = df.sort_values(by=sort_by, ascending=asc, na_position='last')
        st.dataframe(df_sorted.reset_index(drop=True))
        csv_bytes = df_sorted.to_csv(index=False).encode('utf-8')
        st.download_button('CSV をダウンロード', csv_bytes, file_name=os.path.basename(choice))
        if st.sidebar.button('ソート済みCSVを保存'):
            out_name = os.path.join('outputs', 'results', f"sorted_{os.path.basename(choice)}")
            df_sorted.to_csv(out_name, index=False)
            st.success(f'保存しました: `{out_name}`')
    else:
        st.warning('このファイルに `current_price` 列がありません。ローカルキャッシュから価格を取得できます（ネット非使用）。')
        if st.button('ローカルキャッシュで現在価格を取得してソート'):
            prices = []
            progress = st.progress(0)
            status = st.empty()
            tics = df[ticker_col].astype(str).tolist()
            for i, tk in enumerate(tics):
                status.text(f'取得中: {tk} ({i+1}/{len(tics)})')
                p = fetch_price_from_cache(tk)
                prices.append(p)
                progress.progress(int((i + 1) / len(tics) * 100))
            df['current_price'] = prices
            df_sorted = df.sort_values(by='current_price', ascending=True, na_position='last')
            st.dataframe(df_sorted.reset_index(drop=True))
            csv_bytes = df_sorted.to_csv(index=False).encode('utf-8')
            st.download_button('CSV をダウンロード', csv_bytes, file_name=os.path.basename(choice))
            out_name = os.path.join('outputs', 'results', f"{os.path.splitext(os.path.basename(choice))[0]}_with_prices_{datetime.now().date()}.csv")
            df_sorted.to_csv(out_name, index=False)
            st.success(f'保存しました: `{out_name}`')


if __name__ == '__main__':
    main()
