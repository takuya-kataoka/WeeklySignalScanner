#!/usr/bin/env python3
"""
2ドル以下の米国株を検索・スクリーニングするスクリプト（簡易版）
主要銘柄リストから検索
"""
import sys
import os
import config

def main():
    print("=" * 60)
    print("米国株 2ドル以下スクリーナー")
    print("=" * 60)
    print()
    
    # 主要な米国株の広範なリストを用意
    # 実際には数百〜数千の銘柄をチェックする必要がありますが、
    # ここではサンプルとして主要な低価格帯の可能性がある銘柄を含めます
    test_tickers = [
        # Tech
        "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AMD", "INTC",
        # Finance
        "BAC", "WFC", "C", "JPM", "GS", "MS", "USB", "PNC", "TFC",
        # Energy  
        "XOM", "CVX", "COP", "SLB", "HAL", "OXY", "MPC", "VLO", "PSX",
        # Telecom
        "T", "VZ", "TMUS", "CMCSA",
        # Retail
        "WMT", "TGT", "COST", "HD", "LOW", "KSS", "M", "JWN",
        # Auto
        "F", "GM", "STLA", "RIVN", "LCID",
        # Airlines
        "AAL", "UAL", "DAL", "LUV", "JBLU", "ALK",
        # Banks (Regional)
        "KEY", "FITB", "RF", "CFG", "HBAN", "ZION", "CMA", "CBSH",
        # Other potentially low-price
        "NOK", "ERICB.ST", "BB", "SNAP", "PINS", "UBER", "LYFT",
        "GE", "PFE", "KO", "PEP", "DIS", "NFLX", "PYPL",
        # More regional banks and smaller caps
        "ALLY", "SYF", "COF", "DFS", "AXP",
        # Pharma/Bio (some can be low-priced)
        "PFE", "MRK", "ABBV", "BMY", "LLY", "GILD", "BIIB",
        # Additional
        "NIO", "XPEV", "LI", "SOFI", "AFRM", "UPST", "HOOD",
        "PLUG", "FCEL", "BLNK", "CHPT", "CLSK",
        "AMC", "GME", "BBBY", "SNDL", "TLRY", "CGC",
        "VRTX", "REGN", "AMGN", "ISRG",
        # Added stocks that sometimes trade under $10
        "GOLD", "NEM", "FCX", "VALE", "RIG", "CLF", "X", "AA",
        "CCL", "NCLH", "RCL", "MAR", "HLT", "MGM", "WYNN", "LVS",
        "SWN", "AR", "DVN", "FANG", "MRO", "APA", "EOG", "PXD"
    ]
    
    print(f"チェック対象: {len(test_tickers)}銘柄")
    print()
    print("ステップ1: 2ドル以下の銘柄を検索中...")
    print()
    
    from data_fetcher_us import filter_tickers_by_price
    
    # 2ドル以下の銘柄をフィルタリング
    low_price_tickers = filter_tickers_by_price(
        test_tickers,
        max_price=2.0,
        min_price=0.10,  # 極端なペニーストックは除外
        batch_size=20,
        verbose=True
    )
    
    print()
    print("=" * 60)
    print(f"✓ 2ドル以下の銘柄: {len(low_price_tickers)}件")
    print("=" * 60)
    
    if not low_price_tickers:
        print("\n2ドル以下の銘柄が見つかりませんでした。")
        print("別の価格帯を試すか、より多くの銘柄リストを追加してください。")
        
        # 5ドル以下で試してみる
        print("\n参考: 5ドル以下の銘柄を検索してみます...")
        low_price_tickers = filter_tickers_by_price(
            test_tickers,
            max_price=5.0,
            min_price=0.10,
            batch_size=20,
            verbose=False
        )
        print(f"\n5ドル以下の銘柄: {len(low_price_tickers)}件")
        if low_price_tickers:
            for ticker in low_price_tickers[:10]:
                print(f"  - {ticker}")
            if len(low_price_tickers) > 10:
                print(f"  ... 他{len(low_price_tickers)-10}件")
        return
    
    print("\n該当銘柄:")
    for ticker in low_price_tickers:
        print(f"  - {ticker}")
    
    # スクリーニングを実行
    print()
    print("=" * 60)
    print("ステップ2: MA52+陽線包み足スクリーニング実行中...")
    print("=" * 60)
    print()
    
    from screener import scan_stocks
    
    results = scan_stocks(
        low_price_tickers,
        short_window=10,
        long_window=20,
        period="2y",
        interval="1wk",
        threshold=0.0,
        require_ma52=True,
        require_engulfing=True,
    )
    
    print()
    print("=" * 60)
    print("スクリーニング結果")
    print("=" * 60)
    
    if len(results) == 0:
        print("\n条件を満たす銘柄はありませんでした。")
    else:
        print(f"\n✓ {len(results)}件の銘柄が条件を満たしています:")
        print()
        for r in results:
            print(f"  - {r}")
        
        # 結果をCSVに保存（日本語ファイル名）
        import csv
        os.makedirs('outputs/results', exist_ok=True)
        output_path = config.jp_filename('米国_1株2ドル以下')

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ticker'])
            for ticker in results:
                writer.writerow([ticker])
        
        print()
        print(f"✓ 結果を保存しました: {output_path}")
        print()
        print("Streamlitで結果を確認するには:")
        print(f"  streamlit run app_streamlit_us.py")


if __name__ == "__main__":
    main()
