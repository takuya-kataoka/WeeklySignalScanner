#!/usr/bin/env python3
"""
日本株の現在株価を取得して週足スクリーニング条件で抽出
条件: MA52以上 & 陽線包み足
"""
from screener import scan_stocks, generate_jp_tickers_under_price
from datetime import datetime
import csv
import os
import config

def main():
    print("=" * 70)
    print("日本株 現在株価スクリーニング")
    print("条件: 週足MA52以上 & 陽線包み足")
    print("=" * 70)
    print()
    
    print("スキャン範囲を選択:")
    print("1. 主要銘柄（7000-7999, 8000-8999）")
    print("2. 全銘柄（1000-9999）※非常に時間がかかります")
    print("3. 価格フィルタ（1000円以下）")
    print("4. 価格フィルタ（2000円以下）")
    print("5. カスタム範囲")
    
    choice = input("\n選択 (1/2/3/4/5): ").strip()
    
    tickers = []
    
    if choice == "1":
        # 主要銘柄
        print("\n主要銘柄をスキャン中...")
        for start in [7000, 8000]:
            tickers.extend([f"{i:04d}.T" for i in range(start, start + 1000)])
    elif choice == "2":
        # 全銘柄
        print("\n全銘柄をスキャン中（この処理には1時間以上かかる場合があります）...")
        tickers = [f"{i:04d}.T" for i in range(1000, 10000)]
    elif choice == "3":
        # 1000円以下
        print("\n1000円以下の銘柄を検索中...")
        tickers = generate_jp_tickers_under_price(
            max_price=1000,
            start=1000,
            end=9999,
            batch_size=300
        )
        print(f"対象銘柄: {len(tickers)}件")
    elif choice == "4":
        # 2000円以下
        print("\n2000円以下の銘柄を検索中...")
        tickers = generate_jp_tickers_under_price(
            max_price=2000,
            start=1000,
            end=9999,
            batch_size=300
        )
        print(f"対象銘柄: {len(tickers)}件")
    else:
        # カスタム範囲
        start = int(input("開始コード (例: 7200): "))
        end = int(input("終了コード (例: 7300): "))
        tickers = [f"{i:04d}.T" for i in range(start, end + 1)]
    
    print()
    print(f"対象銘柄数: {len(tickers)}")
    print()
    print("スクリーニング実行中...")
    print("（週足データを取得して分析します）")
    print()
    
    # スクリーニング実行
    results = scan_stocks(
        tickers,
        short_window=10,
        long_window=20,
        period="2y",
        interval="1wk",  # 週足
        threshold=0.0,
        require_ma52=True,  # MA52以上
        require_engulfing=True,  # 陽線包み足
    )
    
    print()
    print("=" * 70)
    print("スクリーニング結果")
    print("=" * 70)
    print()
    
    if len(results) == 0:
        print("条件を満たす銘柄はありませんでした。")
        print()
        print("条件:")
        print("  - 週足で最新終値がMA52（52週移動平均）以上")
        print("  - 週足で陽線包み足が発生")
        return
    
    print(f"✓ {len(results)}件の銘柄が条件を満たしています:")
    print()
    
    # 現在の株価情報を取得して表示
    import yfinance as yf
    
    detailed_results = []
    
    print(f"{'銘柄':<12} {'現在価格':<15} {'出来高':<15} {'状態'}")
    print("-" * 70)
    
    for ticker in results:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period='1wk', interval='1wk')
            
            if not hist.empty:
                latest_price = hist['Close'].iloc[-1]
                latest_volume = hist['Volume'].iloc[-1]
                
                # MA52を計算
                hist_long = stock.history(period='2y', interval='1wk')
                if len(hist_long) >= 52:
                    ma52 = hist_long['Close'].rolling(52).mean().iloc[-1]
                    ma_status = f"MA52: ¥{ma52:,.0f}"
                else:
                    ma_status = "MA52: N/A"
                
                print(f"{ticker:<12} ¥{latest_price:>13,.0f} {latest_volume:>13,.0f}  {ma_status}")
                
                detailed_results.append({
                    'ticker': ticker,
                    'price': latest_price,
                    'volume': latest_volume,
                    'ma52': ma52 if len(hist_long) >= 52 else None
                })
            else:
                print(f"{ticker:<12} {'データなし':<15}")
        except Exception as e:
            print(f"{ticker:<12} エラー: {e}")
    
    # CSVに保存（日本語ファイル名）
    os.makedirs('outputs/results', exist_ok=True)
    output_path = config.jp_filename('日本株_現在_MA52_陽線包み')
    
    if detailed_results:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['ticker', 'price', 'volume', 'ma52'])
            writer.writeheader()
            writer.writerows(detailed_results)

        print()
        print("=" * 70)
        print(f"✓ 結果を保存しました: {output_path}")
        print("=" * 70)
        print()
        print("Streamlitで詳細を確認:")
        print("  streamlit run app_streamlit.py")
    else:
        # 銘柄リストだけ保存
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ticker'])
            for ticker in results:
                writer.writerow([ticker])

        print()
        print(f"✓ 銘柄リストを保存: {output_path}")
    
    print()

if __name__ == "__main__":
    main()
