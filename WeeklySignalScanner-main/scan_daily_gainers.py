#!/usr/bin/env python3
"""
今日の米国株値上がり率上位60銘柄を取得して保存
"""
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import csv
import os

def get_sp500_tickers():
    """S&P 500のティッカーリストを取得"""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        sp500_table = tables[0]
        tickers = sp500_table['Symbol'].tolist()
        tickers = [t.replace('.', '-') for t in tickers]
        return tickers
    except Exception as e:
        print(f"S&P 500取得エラー: {e}")
        return []

def get_comprehensive_tickers():
    """包括的な米国株ティッカーリスト"""
    # S&P 500を基本に
    tickers = get_sp500_tickers()
    
    # 主要な追加銘柄
    additional = [
        # Tech
        "NVDA", "AMD", "INTC", "QCOM", "TXN", "ADI", "MRVL", "AVGO",
        "SNOW", "DDOG", "CRWD", "ZS", "NET", "OKTA", "MDB", "TEAM",
        # Finance
        "SOFI", "HOOD", "AFRM", "UPST", "LC", "SQ", "PYPL",
        # EV/Auto
        "TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "F", "GM",
        # Biotech
        "MRNA", "BNTX", "NVAX", "VRTX", "CRSP", "EDIT", "BEAM",
        # Cannabis
        "TLRY", "CGC", "SNDL", "ACB", "CRON", "OGI",
        # Energy
        "PLUG", "FCEL", "BE", "BLNK", "CHPT", "ENPH", "SEDG",
        # Small caps
        "AMC", "GME", "BBBY", "KOSS", "BB", "NOK",
        # Airlines
        "AAL", "UAL", "DAL", "LUV", "JBLU", "ALK",
        # Retail
        "ETSY", "W", "CHWY", "RVLV", "SHOP"
    ]
    
    # 重複を削除して結合
    all_tickers = list(set(tickers + additional))
    return all_tickers

def get_daily_gainers(tickers, top_n=60, verbose=False):
    """
    今日の値上がり率を計算して上位N銘柄を取得
    """
    results = []
    total = len(tickers)
    
    print(f"対象銘柄数: {total}")
    print("値上がり率を計算中...")
    
    batch_size = 50
    for i in range(0, total, batch_size):
        batch = tickers[i:i+batch_size]
        
        if verbose and i % 200 == 0:
            print(f"処理中... {i}/{total}")
        
        try:
            # 過去5日分のデータを取得
            data = yf.download(batch, period='5d', progress=False)
            
            if data.empty:
                continue
            
            # 各銘柄の情報を抽出
            for ticker in batch:
                try:
                    if len(batch) == 1:
                        # 単一銘柄
                        if 'Close' not in data.columns:
                            continue
                        ticker_data = data
                    else:
                        # 複数銘柄
                        if isinstance(data.columns, pd.MultiIndex):
                            if ('Close', ticker) in data.columns:
                                ticker_data = data.xs(ticker, level=1, axis=1)
                            elif (ticker, 'Close') in data.columns:
                                ticker_data = data.xs(ticker, level=0, axis=1)
                            else:
                                continue
                        else:
                            continue
                    
                    # 終値を取得
                    if 'Close' in ticker_data.columns:
                        closes = ticker_data['Close'].dropna()
                    else:
                        closes = ticker_data.dropna()
                    
                    if len(closes) < 2:
                        continue
                    
                    # 最新と前日の終値
                    latest_close = float(closes.iloc[-1])
                    prev_close = float(closes.iloc[-2])
                    
                    # 値上がり率を計算
                    change_pct = ((latest_close - prev_close) / prev_close) * 100
                    
                    # 出来高も取得
                    volume = 0
                    if 'Volume' in ticker_data.columns:
                        volumes = ticker_data['Volume'].dropna()
                        if len(volumes) > 0:
                            volume = float(volumes.iloc[-1])
                    
                    results.append({
                        'ticker': ticker,
                        'price': latest_close,
                        'change_pct': change_pct,
                        'volume': volume
                    })
                    
                except Exception as e:
                    if verbose:
                        print(f"  {ticker}: エラー - {e}")
                    continue
        
        except Exception as e:
            if verbose:
                print(f"Batch error: {e}")
            continue
    
    # 値上がり率でソート
    results.sort(key=lambda x: x['change_pct'], reverse=True)
    
    # 上位N件を返す
    return results[:top_n]

def main():
    print("=" * 70)
    print("米国株 今日の値上がり率上位60銘柄")
    print("=" * 70)
    print()
    
    # ティッカーリストを取得
    tickers = get_comprehensive_tickers()
    print(f"対象銘柄数: {len(tickers)}")
    print()
    
    # 値上がり率上位60を取得
    top_gainers = get_daily_gainers(tickers, top_n=60, verbose=True)
    
    if not top_gainers:
        print("データを取得できませんでした。")
        return
    
    print()
    print("=" * 70)
    print(f"✓ 値上がり率上位{len(top_gainers)}銘柄")
    print("=" * 70)
    print()
    
    # 結果を表示
    print(f"{'順位':<5} {'銘柄':<8} {'価格':<12} {'値上がり率':<12} {'出来高':<15}")
    print("-" * 70)
    
    for i, stock in enumerate(top_gainers, 1):
        print(f"{i:<5} {stock['ticker']:<8} ${stock['price']:>10.2f} {stock['change_pct']:>10.2f}% {stock['volume']:>13,.0f}")
    
    # CSVに保存
    import config
    os.makedirs('outputs/results', exist_ok=True)
    output_path = config.jp_filename('日次_値上がり上位')
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['rank', 'ticker', 'price', 'change_pct', 'volume'])
        writer.writeheader()
        
        for i, stock in enumerate(top_gainers, 1):
            writer.writerow({
                'rank': i,
                'ticker': stock['ticker'],
                'price': stock['price'],
                'change_pct': stock['change_pct'],
                'volume': stock['volume']
            })
    
    print()
    print("=" * 70)
    print(f"✓ 結果を保存しました: {output_path}")
    print("=" * 70)
    
    # 上位10銘柄の詳細情報を表示
    print()
    print("【上位10銘柄の詳細】")
    print()
    
    for i, stock in enumerate(top_gainers[:10], 1):
        try:
            ticker_obj = yf.Ticker(stock['ticker'])
            info = ticker_obj.info
            
            print(f"{i}. {stock['ticker']} - {info.get('longName', 'N/A')}")
            print(f"   価格: ${stock['price']:.2f} (+{stock['change_pct']:.2f}%)")
            print(f"   業種: {info.get('sector', 'N/A')}")
            print(f"   産業: {info.get('industry', 'N/A')}")
            print()
        except:
            print(f"{i}. {stock['ticker']}")
            print(f"   価格: ${stock['price']:.2f} (+{stock['change_pct']:.2f}%)")
            print()

if __name__ == "__main__":
    main()
