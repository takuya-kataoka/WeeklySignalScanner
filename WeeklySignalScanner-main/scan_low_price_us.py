#!/usr/bin/env python3
"""
2ドル以下の米国株を検索・スクリーニングするスクリプト
"""
import sys
import os
import config

def main():
    print("=" * 60)
    print("米国株 2ドル以下スクリーナー")
    print("=" * 60)
    print()
    
    # S&P 500から2ドル以下の銘柄を探す
    print("ステップ1: S&P 500から2ドル以下の銘柄を検索中...")
    print()
    
    from data_fetcher_us import get_sp500_tickers, filter_tickers_by_price
    
    sp500_tickers = get_sp500_tickers()
    print(f"S&P 500銘柄数: {len(sp500_tickers)}")
    
    # 2ドル以下の銘柄をフィルタリング
    low_price_tickers = filter_tickers_by_price(
        sp500_tickers,
        max_price=2.0,
        min_price=0.01,
        batch_size=50,
        verbose=True
    )
    
    print()
    print("=" * 60)
    print(f"✓ 2ドル以下の銘柄: {len(low_price_tickers)}件")
    print("=" * 60)
    
    if not low_price_tickers:
        print("\n2ドル以下の銘柄が見つかりませんでした。")
        print("NASDAQ 100や他の市場を試すこともできます。")
        return
    
    print("\n該当銘柄:")
    for ticker in low_price_tickers:
        print(f"  - {ticker}")
    
    # スクリーニングを実行するか確認
    print()
    print("=" * 60)
    response = input("これらの銘柄に対してMA52+陽線包み足スクリーニングを実行しますか? (y/n): ")
    
    if response.lower() != 'y':
        print("終了します。")
        return
    
    print()
    print("ステップ2: スクリーニング実行中...")
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
        print("条件を満たす銘柄はありませんでした。")
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
