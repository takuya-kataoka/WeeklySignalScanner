import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
from pathlib import Path
import math

st.set_page_config(page_title="米国株週足スクリーナー", layout="wide")

st.title("📈 米国株週足スクリーナー - MA52 & 陽線包み足")

# 結果ファイルを選択
result_files = sorted(Path('outputs/results').glob('us_ma52_engulfing_*.csv'), reverse=True)

if not result_files:
    st.error("米国株の結果ファイルが見つかりません")
    st.info("run_universe_us.py --scan を実行してください")
    st.stop()

selected_file = st.sidebar.selectbox(
    "結果ファイルを選択",
    result_files,
    format_func=lambda x: x.name
)

# データ読み込み
df = pd.read_csv(selected_file)

st.sidebar.metric("検出銘柄数", len(df))

# 銘柄選択
if 'ticker' not in df.columns:
    st.error("ticker列が見つかりません")
    st.stop()

ticker_list = df['ticker'].tolist()

# 表示モード選択
display_mode = st.sidebar.radio("表示モード", ["単一銘柄", "10銘柄一覧"])

if display_mode == "単一銘柄":
    selected_ticker = st.sidebar.selectbox("銘柄を選択", ticker_list)
    selected_tickers = [selected_ticker]
else:
    # 10銘柄ずつページング
    total_pages = math.ceil(len(ticker_list) / 10)
    page = st.sidebar.number_input("ページ", min_value=1, max_value=total_pages, value=1, step=1)
    start_idx = (page - 1) * 10
    end_idx = min(start_idx + 10, len(ticker_list))
    selected_tickers = ticker_list[start_idx:end_idx]
    st.sidebar.info(f"ページ {page}/{total_pages} (銘柄 {start_idx+1}〜{end_idx})")
    
    cols_per_row = 2

# データ取得
@st.cache_data(ttl=3600)
def fetch_data(ticker):
    try:
        data = yf.Ticker(ticker).history(period='2y', interval='1wk')
        if data.empty:
            return None
        return data
    except Exception as e:
        return None

# 選択された銘柄に対してチャート表示
if display_mode == "10銘柄一覧":
    # 2列グリッドレイアウト
    for i in range(0, len(selected_tickers), cols_per_row):
        cols = st.columns(cols_per_row)
        for j, col in enumerate(cols):
            idx = i + j
            if idx >= len(selected_tickers):
                break
            ticker = selected_tickers[idx]
            
            with col:
                data = fetch_data(ticker)
                
                if data is None:
                    st.warning(f"{ticker}: データ取得失敗")
                    continue
                
                # メトリクス表示
                latest_close = data['Close'].iloc[-1]
                ma52 = data['Close'].rolling(52).mean().iloc[-1]
                
                st.markdown(f"**{ticker}**  ${latest_close:,.2f}")
                
                # チャート作成
                fig = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.05,
                    row_heights=[0.75, 0.25]
                )
                
                # ローソク足
                fig.add_trace(
                    go.Candlestick(
                            x=data.index,
                            open=data['Open'],
                            high=data['High'],
                            low=data['Low'],
                            close=data['Close'],
                            name='価格',
                            increasing=dict(line=dict(color='white', width=2.5), fillcolor='green', opacity=0.9),
                            decreasing=dict(line=dict(color='white', width=2.5), fillcolor='red', opacity=0.9),
                            showlegend=False
                        ),
                    row=1, col=1
                )
                
                # MA52
                fig.add_trace(
                    go.Scatter(
                        x=data.index,
                        y=data['Close'].rolling(52).mean(),
                        name='MA52',
                        line=dict(color='orange', width=1),
                        showlegend=False
                    ),
                    row=1, col=1
                )
                
                # 出来高
                colors = ['green' if data['Close'].iloc[k] >= data['Open'].iloc[k] else 'red' 
                          for k in range(len(data))]
                
                fig.add_trace(
                    go.Bar(
                        x=data.index,
                        y=data['Volume'],
                        marker_color=colors,
                        showlegend=False
                    ),
                    row=2, col=1
                )
                
                # レイアウト調整
                fig.update_layout(
                    height=300,
                    margin=dict(l=30, r=10, t=20, b=20),
                    xaxis_rangeslider_visible=False,
                    hovermode='x unified',
                    template='plotly_white',
                    showlegend=False,
                    font=dict(size=8)
                )
                
                fig.update_yaxes(title_text="", row=1, col=1)
                fig.update_yaxes(title_text="", row=2, col=1)
                fig.update_xaxes(showticklabels=False, row=1, col=1)
                fig.update_xaxes(showticklabels=False, row=2, col=1)
                
                st.plotly_chart(fig, use_container_width=True, key=f"chart_grid_{ticker}")

else:
    # 単一銘柄モード
    for ticker in selected_tickers:
        data = fetch_data(ticker)
        
        if data is None:
            st.warning(f"{ticker}: データを取得できませんでした")
            continue
        
        # 区切り線
        st.markdown("---")
        
        # メトリクス表示
        col1, col2, col3, col4, col5 = st.columns(5)
        
        latest_close = data['Close'].iloc[-1]
        latest_volume = data['Volume'].iloc[-1]
        ma52 = data['Close'].rolling(52).mean().iloc[-1]
        change_pct = ((latest_close - data['Close'].iloc[-2]) / data['Close'].iloc[-2] * 100) if len(data) > 1 else 0
        
        with col1:
            st.metric("銘柄", ticker)
        with col2:
            st.metric("株価", f"${latest_close:,.2f}", f"{change_pct:+.2f}%")
        with col3:
            st.metric("出来高", f"{latest_volume:,.0f}")
        with col4:
            st.metric("52週MA", f"${ma52:,.2f}")
        with col5:
            ma_diff_pct = ((latest_close - ma52) / ma52 * 100)
            st.metric("MA52比", f"{ma_diff_pct:+.2f}%")
        
        # チャート作成
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.7, 0.3],
            subplot_titles=(f'{ticker} 週足チャート', '出来高')
        )
        
        # ローソク足
        fig.add_trace(
                go.Candlestick(
                x=data.index,
                open=data['Open'],
                high=data['High'],
                low=data['Low'],
                close=data['Close'],
                name='価格',
                increasing=dict(line=dict(color='white', width=2.5), fillcolor='green', opacity=0.9),
                decreasing=dict(line=dict(color='white', width=2.5), fillcolor='red', opacity=0.9)
            ),
            row=1, col=1
        )
        
        # MA52
        fig.add_trace(
            go.Scatter(
                x=data.index,
                y=data['Close'].rolling(52).mean(),
                name='MA52',
                line=dict(color='orange', width=2)
            ),
            row=1, col=1
        )
        
        # 出来高
        colors = ['green' if data['Close'].iloc[i] >= data['Open'].iloc[i] else 'red' 
                  for i in range(len(data))]
        
        fig.add_trace(
            go.Bar(
                x=data.index,
                y=data['Volume'],
                name='出来高',
                marker_color=colors,
                showlegend=False
            ),
            row=2, col=1
        )
        
        # レイアウト調整
        fig.update_layout(
            height=600,
            xaxis_rangeslider_visible=False,
            hovermode='x unified',
            template='plotly_white',
            showlegend=True
        )
        
        fig.update_yaxes(title_text="株価 ($)", row=1, col=1)
        fig.update_yaxes(title_text="出来高", row=2, col=1)
        fig.update_xaxes(title_text="日付", row=2, col=1)
        
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{ticker}")
        
        # (直近20週のデータ表示は削除しました)

    # 銘柄リスト表示は不要のため非表示
    # (全検出銘柄リストの表示を削除しました)
