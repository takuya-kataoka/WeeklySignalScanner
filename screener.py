import pandas as pd
import yfinance as yf
from utils import get_japan_stock_list

def is_bullish_engulfing(df):
    """陽線包み足の判定（前週陰線、当週陽線、かつ当週の実体が前週を包む）"""
    prev = df.iloc[-2]
    curr = df.iloc[-1]

    return (
        prev["Close"] < prev["Open"] and
        curr["Close"] > curr["Open"] and
        curr["Close"] > prev["Open"] and
        curr["Open"] < prev["Close"]
    )

def check_conditions(ticker):
    data = yf.download(ticker, period="2y", interval="1wk", progress=False)

    if len(data) < 52:
        return False  # データ不足

    # 52週移動平均
    data["MA52"] = data["Close"].rolling(52).mean()

    # 最新週が条件を満たすか確認
    last = data.iloc[-1]

    price_ok = last["Close"] <= 5000
    above_ma = last["Close"] > last["MA52"]
    bullish = is_bullish_engulfing(data)

    return price_ok and above_ma and bullish

def scan_stocks():
    tickers = get_japan_stock_list()
    results = []

    for t in tickers:
        print(f"Checking: {t}...")
        try:
            if check_conditions(t):
                results.append(t)
        except Exception as e:
            print(f"Error {t}: {e}")

    return results

if __name__ == "__main__":
    found = scan_stocks()
    print("\n=== 条件を満たした銘柄 ===")
    for t in found:
        print(t)

