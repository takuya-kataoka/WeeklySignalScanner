from screener import scan_stocks

def main():
    print("=== 日本株スクリーナー ===")  # ここは必ず表示される

    # チェックする銘柄一覧（編集可能）
    tickers = [
        "7203.T", "6758.T", "8306.T", "9984.T", "6861.T",
        "4063.T", "6098.T", "8035.T", "9432.T", "8058.T"
    ]

    # 判定パラメータ（ここを変えると判定が変わります）
    short_window = 10
    long_window = 20
    period = "2y"
    interval = "1wk"
    threshold = 0.0  # 比率での閾値。0.01 = 1% 上回る必要がある

    # 全日本株（例: 1000〜9999の4桁コード）から価格フィルタで対象を決める場合:
    universe_mode = False
    max_price = 1000  # universe_mode が True のときに、終値 <= max_price の銘柄を対象にする

    if universe_mode:
        # 生成には時間がかかる可能性があります。必要に応じて start/end を狭めてください。
        tickers = []
        from screener import generate_jp_tickers_under_price
        tickers = generate_jp_tickers_under_price(max_price=max_price, start=1000, end=9999, batch_size=300)

    # デフォルトは「MA52以上 かつ 陽線包み足」を要求する。必要に応じてフラグで切り替える。
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
