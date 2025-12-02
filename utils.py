import yfinance as yf

def get_japan_stock_list():
    """
    日本株のティッカー一覧を返す。
    例として TOPIX Core30 を使用。
    ※あとで全銘柄に拡張予定
    """
    tickers = [
        "7203.T",  # トヨタ
        "6758.T",  # ソニー
        "8306.T",  # 三菱UFJ
        "9984.T",  # ソフトバンクG
        "6861.T",  # キーエンス
        "4063.T",  # 信越化学
        "6098.T",  # リクルート
        "8035.T",  # 東京エレクトロン
        "9432.T",  # NTT
        "8058.T",  # 三菱商事
    ]
    return tickers

