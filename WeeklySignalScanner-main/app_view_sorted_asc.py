import streamlit as st
import pandas as pd
from pathlib import Path

CSV = "outputs/results/全銘柄_MA52_陽線包み_2025-12-18_with_prices_2025-12-18_sorted_asc.csv"

st.set_page_config(page_title="全銘柄（昇順）", layout="wide")
st.title("全銘柄 MA52 陽線包み — 昇順表示")

p = Path(CSV)
if not p.exists():
    st.error(f"File not found: {CSV}")
else:
    df = pd.read_csv(p)
    st.sidebar.write(f"総件数: {len(df)}")
    # sort controls
    cols = df.columns.tolist()
    default_idx = cols.index('current_price') if 'current_price' in cols else 0
    sort_by = st.sidebar.selectbox('並べ替え列', cols, index=default_idx)
    asc = st.sidebar.checkbox('昇順 (小→大)', value=True)
    df = df.sort_values(by=sort_by, ascending=asc, na_position='last')

    page_size = st.sidebar.selectbox('表示件数', [5, 10, 20, 50], index=1)
    max_page = max(1, (len(df) - 1) // page_size + 1)
    page = st.sidebar.number_input('ページ', min_value=1, max_value=max_page, value=1)
    start = (page - 1) * page_size
    end = start + page_size

    st.write(f"表示: {start+1} - {min(end, len(df))} / {len(df)}")
    st.dataframe(df.iloc[start:end].reset_index(drop=True))

    csv_bytes = df.to_csv(index=False).encode('utf-8')
    st.download_button('CSV をダウンロード', csv_bytes, file_name=p.name, mime='text/csv')
