#!/usr/bin/env python3
"""
日本株の月足で包み足（陽線包み足 & 陰線包み足）を検出
"""
import yfinance as yf
import pandas as pd
from datetime import datetime
import csv
import os

def check_monthly_engulfing(ticker, verbose=False):
    """
    月足で包み足パターンを検出
    
    Returns:
        dict: {
            'ticker': ティッカー,
            'pattern': 'bullish_engulfing' or 'bearish_engulfing' or None,
            'prev_open': 前月始値,
            'prev_close': 前月終値,
            'curr_open': 当月始値,
            'curr_close': 当月終値,
            'latest_price': 最新価格
        }
    """
    try:
        # 月足データを2年分取得
        data = yf.Ticker(ticker).history(period='2y', interval='1mo')
        
        if data is None or data.empty or len(data) < 2:
            if verbose:
                print(f"{ticker}: データ不足")
            return None
        
        # 最新と前月のデータ
        prev = data.iloc[-2]
        curr = data.iloc[-1]
        
        prev_open = float(prev['Open'])
        prev_close = float(prev['Close'])
        prev_high = float(prev['High']) if 'High' in prev else max(prev_open, prev_close)
        prev_low = float(prev['Low']) if 'Low' in prev else min(prev_open, prev_close)
        curr_open = float(curr['Open'])
        curr_close = float(curr['Close'])
        
        # 陽線包み足（Bullish Engulfing）の判定
        # - 前月が陰線（終値 < 始値）
        # - 当月が陽線（終値 > 始値）
        # - 当月の実体が前月の実体を完全に包む（実体が前後で重なれば可）
        is_prev_bearish = prev_close < prev_open
        is_curr_bullish = curr_close > curr_open
        # bullish engulfing: allow touching boundaries (<=, >=)
        bullish_engulfs = (curr_open <= prev_close) and (curr_close >= prev_open)
        # accept also if current body's range covers previous full candle including wicks
        wick_engulf = (curr_open <= prev_low) and (curr_close >= prev_high)
        
        # 陰線包み足（Bearish Engulfing）の判定
        # - 前月が陽線（終値 > 始値）
        # - 当月が陰線（終値 < 始値）
        # - 当月の実体が前月の実体を完全に包む（境界を含む）
        is_prev_bullish = prev_close > prev_open
        is_curr_bearish = curr_close < curr_open
        bearish_engulfs = (curr_open >= prev_close) and (curr_close <= prev_open)
        
        pattern = None
        if is_prev_bearish and is_curr_bullish and (bullish_engulfs or wick_engulf):
            pattern = 'bullish_engulfing'
        elif is_prev_bullish and is_curr_bearish and bearish_engulfs:
            pattern = 'bearish_engulfing'
        
        if pattern:
            return {
                'ticker': ticker,
                'pattern': pattern,
                'prev_open': prev_open,
                'prev_close': prev_close,
                'curr_open': curr_open,
                'curr_close': curr_close,
                'latest_price': curr_close,
                'volume': float(curr['Volume']) if 'Volume' in curr else 0
            }
        
        return None
        
    except Exception as e:
        if verbose:
            print(f"{ticker}: エラー - {e}")
        return None

def get_japanese_tickers(start=1000, end=9999):
    """日本株のティッカーリストを生成"""
    # 除外銘柄
    excluded = {1326, 1543, 1555, 1586, 1593, 1618, 1621, 1672, 1674, 1679, 
                1736, 1795, 1807, 2012, 2013, 1325, 2050, 2250, 1656}
    
    tickers = []
    for code in range(start, end + 1):
        if code not in excluded:
            tickers.append(f"{code:04d}.T")
    
    return tickers

def scan_monthly_engulfing(tickers, verbose=False):
    """
    複数銘柄の月足包み足をスキャン
    """
    bullish_results = []
    bearish_results = []
    
    total = len(tickers)
    print(f"対象銘柄: {total}件")
    print("月足包み足スキャン中...")
    print()
    
    for i, ticker in enumerate(tickers):
        if verbose and i % 100 == 0:
            print(f"進捗: {i}/{total} ({i*100//total}%)")
        
        result = check_monthly_engulfing(ticker, verbose=False)
        
        if result:
            if result['pattern'] == 'bullish_engulfing':
                bullish_results.append(result)
                if verbose:
                    print(f"✓ {ticker}: 陽線包み足")
            elif result['pattern'] == 'bearish_engulfing':
                bearish_results.append(result)
                if verbose:
                    print(f"⚠ {ticker}: 陰線包み足")
    
    return bullish_results, bearish_results

def main():
    print("=" * 70)
    print("日本株 月足包み足スクリーナー")
    print("=" * 70)
    print()
    
    # スキャン範囲を選択
    print("スキャン範囲を選択:")
    print("1. 主要銘柄のみ（7000番台、8000番台など）")
    print("2. 全銘柄（1000-9999）※時間がかかります")
    print("3. カスタム範囲")
    
    choice = input("\n選択 (1/2/3): ").strip()
    
    if choice == "1":
        # 主要銘柄
        tickers = []
        # 7000番台（自動車、電機など）
        tickers.extend([f"{i:04d}.T" for i in range(7200, 7300)])
        # 8000番台（商社、銀行など）
        tickers.extend([f"{i:04d}.T" for i in range(8000, 8100)])
        # 9000番台（通信、電力など）
        tickers.extend([f"{i:04d}.T" for i in range(9400, 9500)])
        # 6000番台（機械など）
        tickers.extend([f"{i:04d}.T" for i in range(6750, 6800)])
        # 4000番台（化学など）
        tickers.extend([f"{i:04d}.T" for i in range(4000, 4100)])
    elif choice == "2":
        # 全銘柄
        tickers = get_japanese_tickers(1000, 9999)
    else:
        # カスタム範囲
        start = int(input("開始コード (例: 7200): "))
        end = int(input("終了コード (例: 7300): "))
        tickers = get_japanese_tickers(start, end)
    
    print()
    print(f"スキャン対象: {len(tickers)}銘柄")
    print()
    
    # スキャン実行
    bullish_results, bearish_results = scan_monthly_engulfing(tickers, verbose=True)
    
    # 結果表示
    print()
    print("=" * 70)
    print("スキャン結果")
    print("=" * 70)
    print()
    
    # 陽線包み足
    print(f"【陽線包み足】 {len(bullish_results)}件")
    print("（前月陰線→当月陽線で包む = 買いシグナル）")
    print("-" * 70)
    
    if bullish_results:
        print(f"{'銘柄':<10} {'最新価格':<12} {'前月':<20} {'当月':<20}")
        print("-" * 70)
        for r in bullish_results:
            prev_candle = f"{r['prev_open']:.0f}→{r['prev_close']:.0f}"
            curr_candle = f"{r['curr_open']:.0f}→{r['curr_close']:.0f}"
            print(f"{r['ticker']:<10} ¥{r['latest_price']:>10,.0f}  {prev_candle:<20} {curr_candle:<20}")
    else:
        print("該当なし")
    
    print()
    print("=" * 70)
    
    # 陰線包み足
    print(f"【陰線包み足】 {len(bearish_results)}件")
    print("（前月陽線→当月陰線で包む = 売りシグナル）")
    print("-" * 70)
    
    if bearish_results:
        print(f"{'銘柄':<10} {'最新価格':<12} {'前月':<20} {'当月':<20}")
        print("-" * 70)
        for r in bearish_results:
            prev_candle = f"{r['prev_open']:.0f}→{r['prev_close']:.0f}"
            curr_candle = f"{r['curr_open']:.0f}→{r['curr_close']:.0f}"
            print(f"{r['ticker']:<10} ¥{r['latest_price']:>10,.0f}  {prev_candle:<20} {curr_candle:<20}")
    else:
        print("該当なし")
    
    # CSVに保存
    import config
    os.makedirs('outputs/results', exist_ok=True)

    if bullish_results:
        output_path = config.jp_filename('月足_陽線包み')
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['ticker', 'pattern', 'latest_price', 
                                                     'prev_open', 'prev_close', 
                                                     'curr_open', 'curr_close', 'volume'])
            writer.writeheader()
            writer.writerows(bullish_results)
        print()
        print(f"✓ 陽線包み足を保存: {output_path}")
    
    if bearish_results:
        output_path = config.jp_filename('月足_陰線包み')
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['ticker', 'pattern', 'latest_price',
                                                     'prev_open', 'prev_close',
                                                     'curr_open', 'curr_close', 'volume'])
            writer.writeheader()
            writer.writerows(bearish_results)
        print(f"✓ 陰線包み足を保存: {output_path}")
    
    print()
    print("=" * 70)

if __name__ == "__main__":
    main()
