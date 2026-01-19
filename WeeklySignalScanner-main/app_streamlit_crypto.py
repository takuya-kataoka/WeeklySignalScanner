import os
import json
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objs as go
from datetime import datetime


COINGECKO_BASE = "https://api.coingecko.com/api/v3"
FAV_PATH = os.path.join(os.path.dirname(__file__), "data", "crypto_favorites.json")

# サポートするチェーン（CoinGecko のプラットフォーム名）
COINGECKO_PLATFORMS = [
    "ethereum",
    "binance-smart-chain",
    "polygon-pos",
    "fantom",
    "avalanche",
    "arbitrum",
]


def fetch_coin_by_contract(platform: str, contract_address: str):
    # CoinGecko の /coins/{platform}/contract/{contract_address} を利用
    url = f"{COINGECKO_BASE}/coins/{platform}/contract/{contract_address}"
    r = requests.get(url)
    r.raise_for_status()
    j = r.json()
    # 最低限必要な情報を返す: id, name, symbol
    return {"id": j.get("id"), "name": j.get("name"), "symbol": j.get("symbol"), "platform": platform, "contract": contract_address}


def load_favorites():
    try:
        if not os.path.exists(os.path.dirname(FAV_PATH)):
            os.makedirs(os.path.dirname(FAV_PATH), exist_ok=True)
        if not os.path.exists(FAV_PATH):
            return []
        with open(FAV_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_favorites(favs):
    try:
        with open(FAV_PATH, "w", encoding="utf-8") as f:
            json.dump(favs, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def search_coins(query: str):
    r = requests.get(f"{COINGECKO_BASE}/search", params={"query": query})
    r.raise_for_status()
    return r.json().get("coins", [])


def fetch_market_chart(coin_id: str, vs_currency: str = "usd"):
    # days=max を使い長期の履歴を取得し、週足にリサンプリングする
    r = requests.get(f"{COINGECKO_BASE}/coins/{coin_id}/market_chart", params={"vs_currency": vs_currency, "days": "max"})
    r.raise_for_status()
    j = r.json()
    prices = j.get("prices", [])
    vols = j.get("total_volumes", [])

    if not prices:
        return None

    df = pd.DataFrame(prices, columns=["ts", "price"])  # ts in ms
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df = df.set_index("ts").sort_index()

    # 日次の連続時系列を作り、週足に変換（OHLC）
    daily = df["price"].resample("D").ffill()
    weekly_ohlc = daily.resample("W-SUN").agg(["first", "max", "min", "last"]).dropna()
    weekly_ohlc.columns = ["open", "high", "low", "close"]

    # ボリュームは total_volumes を日次にして週で合算
    vdf = pd.DataFrame(vols, columns=["ts", "volume"]) if vols else None
    if vdf is not None and not vdf.empty:
        vdf["ts"] = pd.to_datetime(vdf["ts"], unit="ms")
        vdf = vdf.set_index("ts").sort_index()
        daily_vol = vdf["volume"].resample("D").ffill().fillna(0)
        weekly_vol = daily_vol.resample("W-SUN").sum()
        weekly_ohlc["volume"] = weekly_vol.reindex(weekly_ohlc.index).fillna(0)
    else:
        weekly_ohlc["volume"] = 0

    # MA52（52週移動平均）
    weekly_ohlc["ma52"] = weekly_ohlc["close"].rolling(window=52).mean()

    return weekly_ohlc


def plot_weekly(weekly_df: pd.DataFrame, title: str):
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=weekly_df.index,
        open=weekly_df["open"],
        high=weekly_df["high"],
        low=weekly_df["low"],
        close=weekly_df["close"],
        name="週足"
    ))
    fig.add_trace(go.Bar(x=weekly_df.index, y=weekly_df["volume"], name="出来高", yaxis="y2", marker_color="lightgrey", opacity=0.6))
    fig.add_trace(go.Scatter(x=weekly_df.index, y=weekly_df["ma52"], mode="lines", name="MA52", line=dict(color="blue")))

    # レイアウトで出来高を右側に別軸で描画
    fig.update_layout(
        title=title,
        xaxis=dict(rangeslider=dict(visible=False)),
        yaxis=dict(title="価格 (USD)"),
        yaxis2=dict(title="出来高", overlaying="y", side="right", showgrid=False, position=1.0)
    )
    return fig


def main():
    st.set_page_config(page_title="Crypto 週足チャート", layout="wide")
    st.title("📈 Crypto 週足チャート (CoinGecko)")

    st.sidebar.header("検索と設定")
    # お気に入りの読み込み
    favorites = load_favorites()
    st.sidebar.subheader("お気に入り")
    fav_options = []
    for c in favorites:
        if c.get("contract"):
            fav_options.append(f"{c.get('name')} ({c.get('symbol')}) — contract:{c.get('contract')} on {c.get('platform')}")
        else:
            fav_options.append(f"{c.get('name')} ({c.get('symbol')}) — id:{c.get('id')}")
    fav_choice = None
    if fav_options:
        fav_choice = st.sidebar.selectbox("お気に入りから選択", ["-- 選択 --"] + fav_options)

    # 検索方式: 名前/ティッカー or コントラクト
    search_mode = st.sidebar.radio("検索方式", ["名前/ティッカー", "コントラクトアドレス"], index=0)
    query = None
    contract_input = None
    contract_platform = None
    if search_mode == "名前/ティッカー":
        query = st.sidebar.text_input("コイン名またはティッカーを入力 (例: 114514, doge, shib)")
    else:
        contract_platform = st.sidebar.selectbox("チェーンを選択", COINGECKO_PLATFORMS)
        contract_input = st.sidebar.text_input("コントラクトアドレスを入力 (0x...)")

    vs_currency = st.sidebar.selectbox("表示通貨", ["usd", "jpy"], index=0)

    selected_coin = None
    if fav_choice and fav_choice != "-- 選択 --":
        idx = fav_options.index(fav_choice)
        selected_coin = favorites[idx]

    # 検索ボタン押下時の処理 (名前検索 or contract 検索)
    if st.sidebar.button("検索"):
        if search_mode == "名前/ティッカー":
            if not query:
                st.warning("検索語を入力してください")
            else:
                with st.spinner("CoinGecko を検索しています..."):
                    try:
                        hits = search_coins(query)
                    except Exception as e:
                        st.error(f"検索でエラー: {e}")
                        hits = []
                if not hits:
                    st.warning("該当するコインが見つかりませんでした。別の語で試してください。")
                else:
                    options = [f"{h['name']} ({h['symbol']}) — id:{h['id']}" for h in hits]
                    choice = st.selectbox("検索結果から選択", options)
                    idx = options.index(choice)
                    coin = hits[idx]
                    st.write(f"選択: **{coin['name']}** (id: `{coin['id']}`, symbol: `{coin['symbol']}`)")
        else:
            # contract 検索
            if not contract_input:
                st.warning("コントラクトアドレスを入力してください")
            else:
                with st.spinner("コントラクトからコイン情報を取得しています..."):
                    try:
                        coin = fetch_coin_by_contract(contract_platform, contract_input.strip())
                    except Exception as e:
                        st.error(f"コントラクト検索でエラー: {e}")
                        coin = None
                if not coin or not coin.get("id"):
                    st.warning("該当するコインが見つかりませんでした（CoinGecko に登録されていない可能性があります）。")
                else:
                    st.write(f"選択: **{coin['name']}** (id: `{coin['id']}`, symbol: `{coin['symbol']}`)")

        # ここに来るのは、検索処理で coin が設定された場合
        try:
            coin
        except NameError:
            coin = None

        if coin:
            # お気に入りに追加ボタン
            if st.button("お気に入りに追加"):
                # contract 情報があればそれを保存
                entry = {"id": coin.get("id"), "name": coin.get("name"), "symbol": coin.get("symbol")}
                if coin.get("contract"):
                    entry.update({"platform": coin.get("platform"), "contract": coin.get("contract")})
                exists = False
                for c in favorites:
                    if c.get("id") == entry.get("id") and c.get("contract") == entry.get("contract"):
                        exists = True
                        break
                if not exists:
                    favorites.append(entry)
                    save_favorites(favorites)
                    st.success("お気に入りに追加しました")
                else:
                    st.info("既にお気に入りに登録済みです")

            if st.button("チャート表示"):
                with st.spinner("履歴データを取得しています... (CoinGecko API)"):
                    try:
                        df = fetch_market_chart(coin["id"], vs_currency=vs_currency)
                    except Exception as e:
                        st.error(f"データ取得エラー: {e}")
                        return

                if df is None or df.empty:
                    st.warning("チャートデータが取得できませんでした。")
                    return

                st.subheader("週足チャート")
                fig = plot_weekly(df, title=f"{coin['name']} 週足 ({vs_currency.upper()})")
                st.plotly_chart(fig, use_container_width=True)

                st.subheader("最新データ")
                last = df.iloc[-1]
                st.write({
                    "週終了日": str(df.index[-1].date()),
                    "終値": float(last["close"]),
                    "始値": float(last["open"]),
                    "高値": float(last["high"]),
                    "安値": float(last["low"]),
                    "出来高(週合計)": float(last["volume"]),
                    "MA52": float(last["ma52"]) if not pd.isna(last["ma52"]) else None
                })

    # サイドバーでお気に入りから直接ロードした場合の処理
    if selected_coin is not None:
        st.write(f"選択: **{selected_coin['name']}** (id: `{selected_coin['id']}`, symbol: `{selected_coin['symbol']}`)")
        if st.sidebar.button("お気に入りのチャート表示"):
            with st.spinner("履歴データを取得しています... (CoinGecko API)"):
                try:
                    df = fetch_market_chart(selected_coin["id"], vs_currency=vs_currency)
                except Exception as e:
                    st.error(f"データ取得エラー: {e}")
                    df = None

            if df is None or df.empty:
                st.warning("チャートデータが取得できませんでした。")
            else:
                st.subheader("週足チャート")
                fig = plot_weekly(df, title=f"{selected_coin['name']} 週足 ({vs_currency.upper()})")
                st.plotly_chart(fig, use_container_width=True)

                st.subheader("最新データ")
                last = df.iloc[-1]
                st.write({
                    "週終了日": str(df.index[-1].date()),
                    "終値": float(last["close"]),
                    "始値": float(last["open"]),
                    "高値": float(last["high"]),
                    "安値": float(last["low"]),
                    "出来高(週合計)": float(last["volume"]),
                    "MA52": float(last["ma52"]) if not pd.isna(last["ma52"]) else None
                })


if __name__ == "__main__":
    main()
