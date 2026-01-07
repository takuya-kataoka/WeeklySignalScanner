from screener import scan_stocks

def main():
    print("=== 米国株スクリーナー ===")

    # チェックする米国株銘柄一覧（編集可能）
    # 主要な米国株のティッカーシンボル
    tickers = [
        # Tech Giants (FAANG+)
        "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
        # Other Major Tech
        "AMD", "INTC", "NFLX", "CRM", "ORCL", "ADBE", "CSCO",
        # Finance
        "JPM", "BAC", "WFC", "GS", "MS", "V", "MA",
        # Healthcare
        "JNJ", "UNH", "PFE", "ABBV", "TMO", "MRK", "ABT",
        # Consumer
        "WMT", "DIS", "NKE", "SBUX", "MCD", "HD", "LOW",
        # Industrial
        "BA", "CAT", "GE", "MMM", "UPS", "HON",
        # Energy
        "XOM", "CVX", "COP", "SLB",
        # Communication
        "T", "VZ", "CMCSA"
    ]

    # 判定パラメータ
    short_window = 10
    long_window = 20
    period = "2y"
    interval = "1wk"
    threshold = 0.0

    # デフォルトは「MA52以上 かつ 陽線包み足」を要求
    results = scan_stocks(
        tickers,
        short_window=short_window,
        long_window=long_window,
        period=period,
        interval=interval,
        threshold=threshold,
        require_ma52=True,
        require_engulfing=True,
    )

    print("\n=== 該当銘柄 ===")
    if len(results) == 0:
        print("該当銘柄なし…")
    else:
        for r in results:
            print(f"- {r}")

if __name__ == "__main__":
    main()
