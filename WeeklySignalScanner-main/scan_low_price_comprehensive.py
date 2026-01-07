#!/usr/bin/env python3
"""
2ドル以下の米国株を広範囲から検索するスクリプト
NYSE、NASDAQ上場の主要銘柄を含む
"""
import sys
import os
import config

def get_comprehensive_ticker_list():
    """
    より包括的な米国株ティッカーリストを生成
    主要取引所の代表的な銘柄を含む
    """
    tickers = []
    
    # 主要大型株
    large_caps = [
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
        "LLY", "V", "UNH", "XOM", "JPM", "JNJ", "AVGO", "WMT", "MA", "PG", "HD",
        "ORCL", "COST", "NFLX", "CRM", "BAC", "ABBV", "CVX", "MRK", "KO", "PEP",
        "TMO", "CSCO", "AMD", "ACN", "LIN", "MCD", "WFC", "ABT", "ADBE", "DHR",
        "NKE", "TXN", "DIS", "INTU", "VZ", "PM", "IBM", "AMGN", "GE", "QCOM"
    ]
    
    # 中型株
    mid_caps = [
        "BKNG", "ISRG", "AXP", "HON", "CAT", "NOW", "SPGI", "T", "GS", "MS",
        "UNP", "DE", "BA", "RTX", "PLD", "VRTX", "BLK", "SYK", "GILD", "LRCX",
        "MMM", "ADI", "TJX", "MDLZ", "REGN", "C", "SBUX", "PYPL", "AMT", "TMUS",
        "AMAT", "INTC", "LOW", "PFE", "CB", "SO", "SCHW", "BMY", "ELV", "CI"
    ]
    
    # 金融・銀行
    financials = [
        "USB", "PNC", "TFC", "BK", "COF", "AIG", "MET", "PRU", "ALL", "AMP",
        "FITB", "RF", "KEY", "CFG", "HBAN", "CMA", "ZION", "MTB", "SIVB", "CBSH",
        "FRC", "WAL", "WTFC", "ONB", "UBSI", "FFIN", "FNB", "SNV", "UMBF", "PB",
        "ALLY", "SYF", "DFS", "COF", "SOFI", "LC", "UPST", "AFRM"
    ]
    
    # エネルギー
    energy = [
        "SLB", "HAL", "OXY", "MPC", "VLO", "PSX", "COP", "EOG", "PXD", "DVN",
        "FANG", "MRO", "APA", "HES", "OVV", "CTRA", "NOG", "PR", "MGY", "SM",
        "AR", "MTDR", "CLR", "RRC", "SWN", "CNX", "CHK", "RIG", "VAL", "HP",
        "DO", "NE", "PTEN", "LBRT", "NINE", "NBR", "WHD", "WTTR", "TDW", "PUMP"
    ]
    
    # 小売・消費財
    retail = [
        "TGT", "AMZN", "EBAY", "ETSY", "W", "CHWY", "RVLV", "FTCH", "REAL", "GRPN",
        "KSS", "M", "JWN", "BBBY", "DKS", "FIVE", "DLTR", "DG", "BURL", "ROST",
        "TJX", "GPS", "ANF", "AEO", "URBN", "EXPR", "ZUMZ", "GES", "PLCE", "CATO"
    ]
    
    # テクノロジー（小型含む）
    tech = [
        "SNAP", "PINS", "TWTR", "SPOT", "HOOD", "RBLX", "U", "DDOG", "SNOW", "NET",
        "ZS", "CRWD", "OKTA", "MDB", "TEAM", "TWLO", "ZM", "DOCU", "SPLK", "WDAY",
        "PANW", "FTNT", "CYBR", "TENB", "S", "RPD", "BILL", "SMAR", "AI", "GTLB",
        "MNDY", "IOT", "FROG", "PATH", "APPN", "ESTC", "COUP", "RNG", "NCNO", "DT"
    ]
    
    # 自動車・EV
    auto = [
        "F", "GM", "STLA", "RIVN", "LCID", "NKLA", "FSR", "GOEV", "RIDE", "WKHS",
        "NIO", "XPEV", "LI", "BYDDY", "HYMTF", "TSLA", "PTRA", "BLNK", "CHPT", "EVGo"
    ]
    
    # 航空・運輸
    transport = [
        "AAL", "UAL", "DAL", "LUV", "ALK", "JBLU", "SAVE", "HA", "MESA", "SKYW",
        "UPS", "FDX", "XPO", "JBHT", "KNX", "ODFL", "SAIA", "ARCB", "CVLG", "WERN"
    ]
    
    # バイオ・製薬（小型含む）
    biotech = [
        "MRNA", "BNTX", "VRTX", "ALNY", "BMRN", "IONS", "RGEN", "TECH", "SRPT", "RARE",
        "FOLD", "BLUE", "CRSP", "EDIT", "NTLA", "BEAM", "VERV", "ARCT", "MRVI", "AGEN",
        "DVAX", "NVAX", "INO", "OCGN", "VXRT", "SAVA", "ABUS", "ADMA", "ATNF", "CTIC"
    ]
    
    # 大麻関連
    cannabis = [
        "TLRY", "CGC", "SNDL", "ACB", "CRON", "OGI", "HEXO", "CURLF", "GTBIF", "TCNNF",
        "CRLBF", "TRSSF", "VRNOF", "AYRWF", "GRAMF", "JUSHF", "PLNHF", "HRVSF", "CCHWF"
    ]
    
    # 再生可能エネルギー
    clean_energy = [
        "PLUG", "FCEL", "BE", "BLDP", "RUN", "ENPH", "SEDG", "NOVA", "CSIQ", "JKS",
        "SPWR", "MAXN", "ARRAY", "VSLR", "NEE", "AES", "DUK", "SO", "D", "EXC"
    ]
    
    # 通信
    telecom = [
        "NOK", "ERIC", "VZ", "T", "TMUS", "LUMN", "CABO", "SHEN", "ATUS", "SATS",
        "GOGO", "VSAT", "GILT", "IRDM", "ORBC", "CMCSA", "CHTR", "DISH", "LITE"
    ]
    
    # 鉱業・金属
    metals = [
        "GOLD", "NEM", "FCX", "AA", "X", "CLF", "STLD", "NUE", "MT", "VALE",
        "RIO", "BHP", "SCCO", "TECK", "HBM", "CDE", "HL", "AUY", "EGO", "IAG",
        "KGC", "BTG", "AGI", "PAAS", "WPM", "FNV", "RGLD", "OR", "SAND", "MAG"
    ]
    
    # 不動産・REIT
    reits = [
        "AMT", "PLD", "CCI", "EQIX", "PSA", "SPG", "O", "WELL", "DLR", "AVB",
        "EQR", "VTR", "ARE", "INVH", "MAA", "ESS", "UDR", "CPT", "KIM", "REG"
    ]
    
    # エンターテインメント・メディア
    entertainment = [
        "DIS", "NFLX", "PARA", "WBD", "FOXA", "CMCSA", "LYV", "MSG", "MSGS", "IMAX",
        "CNK", "AMC", "CINE", "RGC", "MARK", "NRDY", "GNUS", "EMAN", "FUNFF", "YVR"
    ]
    
    # その他注目小型株
    small_caps = [
        "GME", "BBBY", "EXPR", "KOSS", "BB", "NAKD", "CTRM", "SHIP", "TOPS", "GLBS",
        "VEON", "ATOS", "ZSAN", "JAGX", "IDEX", "XELA", "WTRH", "MRIN", "CARV", "BKSY"
    ]
    
    # すべてを結合
    tickers = (large_caps + mid_caps + financials + energy + retail + tech + 
               auto + transport + biotech + cannabis + clean_energy + telecom + 
               metals + reits + entertainment + small_caps)
    
    # 重複を削除
    tickers = list(set(tickers))
    
    return tickers


def main():
    print("=" * 70)
    print("米国株 2ドル以下スクリーナー（広範囲検索）")
    print("=" * 70)
    print()
    
    # 包括的なティッカーリストを取得
    all_tickers = get_comprehensive_ticker_list()
    
    print(f"検索対象: {len(all_tickers)}銘柄")
    print("※ NYSE、NASDAQ、その他主要取引所の銘柄を含みます")
    print()
    print("ステップ1: 2ドル以下の銘柄を検索中...")
    print("（この処理には数分かかる場合があります）")
    print()
    
    from data_fetcher_us import filter_tickers_by_price
    
    # 2ドル以下の銘柄をフィルタリング
    low_price_tickers = filter_tickers_by_price(
        all_tickers,
        max_price=2.0,
        min_price=0.10,  # 極端なペニーストックは除外
        batch_size=30,
        verbose=True
    )
    
    print()
    print("=" * 70)
    print(f"✓ 2ドル以下の銘柄: {len(low_price_tickers)}件")
    print("=" * 70)
    
    if not low_price_tickers:
        print("\n2ドル以下の銘柄が見つかりませんでした。")
        return
    
    print("\n該当銘柄:")
    for ticker in sorted(low_price_tickers):
        print(f"  - {ticker}")
    
    # 結果をファイルに保存
    print()
    with open('outputs/low_price_tickers.txt', 'w') as f:
        for ticker in sorted(low_price_tickers):
            f.write(f"{ticker}\n")
    print("✓ 銘柄リストを保存: outputs/low_price_tickers.txt")
    
    # スクリーニングを実行
    print()
    print("=" * 70)
    print("ステップ2: MA52+陽線包み足スクリーニング実行中...")
    print("=" * 70)
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
    print("=" * 70)
    print("スクリーニング結果")
    print("=" * 70)
    
    if len(results) == 0:
        print("\n条件を満たす銘柄はありませんでした。")
        print("（MA52以上 かつ 陽線包み足の条件）")
    else:
        print(f"\n✓ {len(results)}件の銘柄が条件を満たしています:")
        print()
        for r in sorted(results):
            print(f"  - {r}")
        
        # 結果をCSVに保存（日本語ファイル名）
        import csv
        os.makedirs('outputs/results', exist_ok=True)
        output_path = config.jp_filename('米国_1株2ドル以下')

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ticker'])
            for ticker in sorted(results):
                writer.writerow([ticker])
        
        print()
        print(f"✓ 結果を保存しました: {output_path}")
        print()
        print("Streamlitで結果を確認するには:")
        print(f"  streamlit run app_streamlit_us.py")


if __name__ == "__main__":
    main()
