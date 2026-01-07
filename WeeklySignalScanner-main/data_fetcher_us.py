import os
import time
import yfinance as yf
import pandas as pd

# 米国株用のデータ取得モジュール
# 日本株と異なり、除外銘柄リストは空（必要に応じて追加）
EXCLUDED_TICKERS = set()


def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def fetch_and_save_us_tickers(tickers, batch_size=100, period='6mo', interval='1d', out_dir='data_us', retry_count=2, sleep_between_batches=1.0, verbose=False):
    """
    指定された米国株ティッカーリストをバッチで取得して、各ティッカーごとに Parquet ファイルとして保存します。

    Args:
        tickers: 米国株のティッカーシンボルのリスト (例: ['AAPL', 'MSFT', 'GOOGL'])
        batch_size: 一度に取得する銘柄数
        period: yfinanceに渡す期間 (例: '6mo', '1y', '2y')
        interval: データ間隔 (例: '1d', '1wk')
        out_dir: 保存先ディレクトリ
        retry_count: リトライ回数
        sleep_between_batches: バッチ間のスリープ時間（秒）
        verbose: 詳細ログを出力するか
    """
    _ensure_dir(out_dir)

    # 除外銘柄をフィルタ
    all_tickers = [t for t in tickers if t not in EXCLUDED_TICKERS]
    total = len(all_tickers)
    total_batches = (total - 1) // batch_size + 1

    for batch_idx, i in enumerate(range(0, total, batch_size), start=1):
        batch = all_tickers[i:i+batch_size]
        if verbose:
            print(f"Fetching US batch {batch_idx}/{total_batches} (size={len(batch)})")

        attempt = 0
        df = None
        while attempt <= retry_count:
            try:
                df = yf.download(batch, period=period, interval=interval, progress=False, group_by='ticker', auto_adjust=False)
                break
            except Exception as e:
                attempt += 1
                if attempt > retry_count:
                    if verbose:
                        print(f"batch download failed after {attempt} attempts: {e}")
                else:
                    wait = sleep_between_batches * (2 ** (attempt - 1))
                    if verbose:
                        print(f"batch download error, retrying after {wait}s: {e}")
                    time.sleep(wait)

        # If batch df is empty, fallback to per-ticker
        if df is None or (hasattr(df, 'empty') and df.empty):
            if verbose:
                print("Batch empty — falling back to per-ticker fetch")
            for t in batch:
                try:
                    single = yf.Ticker(t).history(period=period, interval=interval)
                    if single is None or getattr(single, 'empty', True):
                        if verbose:
                            print(f"{t}: no data")
                        continue
                    cols = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in single.columns]
                    valid = single.dropna(subset=['Close']) if 'Close' in single.columns else single
                    if valid is None or getattr(valid, 'empty', True):
                        if verbose:
                            print(f"{t}: no valid Close values, skipping save")
                        continue
                    path = os.path.join(out_dir, f"{t}.parquet")
                    single[cols].to_parquet(path)
                    if verbose:
                        print(f"Saved {t} -> {path}")
                except Exception as e:
                    if verbose:
                        print(f"{t}: fetch error {e}")
                time.sleep(sleep_between_batches)
            continue

        # Parse batch df and save per-ticker files
        for t in batch:
            try:
                series_df = None
                if isinstance(df.columns, pd.MultiIndex):
                    if ('Close', t) in df.columns:
                        series_df = df.xs(t, level=1, axis=1)
                    elif (t, 'Close') in df.columns:
                        series_df = df.xs(t, level=0, axis=1)
                    else:
                        cols = [c for c in df.columns if t in str(c)]
                        if cols:
                            series_df = df[cols]
                else:
                    possible_cols = [c for c in df.columns if t in str(c)]
                    if possible_cols:
                        series_df = df[possible_cols]

                if series_df is None or getattr(series_df, 'empty', True):
                    single = yf.Ticker(t).history(period=period, interval=interval)
                    if single is None or getattr(single, 'empty', True):
                        if verbose:
                            print(f"{t}: no data in batch or single")
                        continue
                    cols = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in single.columns]
                    valid = single.dropna(subset=['Close'])
                    if valid is None or getattr(valid, 'empty', True):
                        if verbose:
                            print(f"{t}: no valid Close")
                        continue
                    path = os.path.join(out_dir, f"{t}.parquet")
                    single[cols].to_parquet(path)
                    if verbose:
                        print(f"Saved {t} (fallback) -> {path}")
                    continue

                # flatten multiindex columns if needed
                if isinstance(series_df.columns, pd.MultiIndex):
                    series_df.columns = ['_'.join(map(str, col)).strip('_') for col in series_df.columns.values]

                cols = [c for c in series_df.columns if any(x in c for x in ['Open', 'High', 'Low', 'Close', 'Volume'])]
                if not cols:
                    if verbose:
                        print(f"{t}: no OHLCV columns")
                    continue

                # rename columns to standard names if needed
                rename_map = {}
                for c in cols:
                    if 'Open' in c:
                        rename_map[c] = 'Open'
                    elif 'High' in c:
                        rename_map[c] = 'High'
                    elif 'Low' in c:
                        rename_map[c] = 'Low'
                    elif 'Close' in c:
                        rename_map[c] = 'Close'
                    elif 'Volume' in c:
                        rename_map[c] = 'Volume'
                
                series_df = series_df.rename(columns=rename_map)
                valid = series_df.dropna(subset=['Close'])
                
                if valid is None or getattr(valid, 'empty', True):
                    if verbose:
                        print(f"{t}: no valid Close in batch df")
                    continue

                path = os.path.join(out_dir, f"{t}.parquet")
                valid[['Open', 'High', 'Low', 'Close', 'Volume']].to_parquet(path)
                if verbose:
                    print(f"Saved {t} -> {path}")
            except Exception as e:
                if verbose:
                    print(f"{t}: save error {e}")

        if verbose:
            print(f"Batch {batch_idx} complete. Sleeping {sleep_between_batches}s...")
        time.sleep(sleep_between_batches)

    if verbose:
        print("All US tickers fetch complete!")


def get_sp500_tickers():
    """
    S&P 500のティッカーリストを取得する（Wikipediaから）
    """
    try:
        import pandas as pd
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        sp500_table = tables[0]
        tickers = sp500_table['Symbol'].tolist()
        # Clean up tickers (remove dots, etc.)
        tickers = [t.replace('.', '-') for t in tickers]
        return tickers
    except Exception as e:
        print(f"Failed to fetch S&P 500 tickers: {e}")
        return []


def get_nasdaq100_tickers():
    """
    NASDAQ 100の主要ティッカーリスト（手動リスト）
    完全なリストが必要な場合はAPIやスクレイピングを使用
    """
    return [
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
        "AVGO", "COST", "ASML", "PEP", "AZN", "CSCO", "ADBE", "TMUS",
        "CMCSA", "NFLX", "AMD", "INTC", "INTU", "TXN", "QCOM", "HON",
        "AMGN", "AMAT", "ISRG", "BKNG", "SBUX", "GILD", "ADI", "VRTX",
        "ADP", "REGN", "MDLZ", "LRCX", "PANW", "PYPL", "MU", "SNPS",
        "KLAC", "MELI", "CDNS", "MAR", "CTAS", "NXPI", "ORLY", "CRWD",
        "MNST", "ADSK", "ABNB", "CSX", "WDAY", "FTNT", "MRVL", "DASH",
        "TEAM", "DXCM", "AEP", "PCAR", "ODFL", "KDP", "CPRT", "CHTR",
        "PAYX", "ROST", "MCHP", "FAST", "TTD", "EA", "CTSH", "KHC",
        "IDXX", "ON", "VRSK", "LULU", "EXC", "CCEP", "DDOG", "ANSS",
        "XEL", "CSGP", "CDW", "BIIB", "WBD", "ZS", "GEHC", "MDB",
        "TTWO", "ILMN", "GFS", "FANG", "WBA", "MRNA", "SMCI", "ARM"
    ]


def filter_tickers_by_price(tickers, max_price=2.0, min_price=0.01, batch_size=100, verbose=False):
    """
    指定された価格範囲内の銘柄のみをフィルタリング
    
    Args:
        tickers: チェックする銘柄リスト
        max_price: 最大価格（デフォルト: 2.0ドル）
        min_price: 最小価格（デフォルト: 0.01ドル、ペニーストック除外用）
        batch_size: 一度に取得する銘柄数
        verbose: 詳細ログ
    
    Returns:
        価格範囲内の銘柄リスト
    """
    filtered = []
    total = len(tickers)
    
    if verbose:
        print(f"Filtering {total} tickers for price range ${min_price} - ${max_price}")
    
    for i in range(0, total, batch_size):
        batch = tickers[i:i+batch_size]
        
        try:
            # バッチでダウンロード
            data = yf.download(batch, period='5d', progress=False)
            
            if data.empty:
                # フォールバック: 個別取得
                for ticker in batch:
                    try:
                        tick = yf.Ticker(ticker)
                        hist = tick.history(period='5d')
                        if not hist.empty:
                            latest_price = hist['Close'].iloc[-1]
                            if min_price <= latest_price <= max_price:
                                filtered.append(ticker)
                                if verbose:
                                    print(f"  ✓ {ticker}: ${latest_price:.2f}")
                    except Exception as e:
                        if verbose:
                            print(f"  ✗ {ticker}: エラー {e}")
                continue
            
            # バッチデータを解析
            for ticker in batch:
                try:
                    if len(batch) == 1:
                        # 単一銘柄
                        if 'Close' in data.columns:
                            latest_price = data['Close'].iloc[-1]
                        else:
                            continue
                    else:
                        # 複数銘柄
                        if isinstance(data.columns, pd.MultiIndex):
                            if ('Close', ticker) in data.columns:
                                latest_price = data[('Close', ticker)].iloc[-1]
                            elif (ticker, 'Close') in data.columns:
                                latest_price = data[(ticker, 'Close')].iloc[-1]
                            else:
                                continue
                        else:
                            continue
                    
                    if pd.notna(latest_price) and min_price <= latest_price <= max_price:
                        filtered.append(ticker)
                        if verbose:
                            print(f"  ✓ {ticker}: ${latest_price:.2f}")
                    elif verbose and pd.notna(latest_price):
                        print(f"  - {ticker}: ${latest_price:.2f} (範囲外)")
                        
                except Exception as e:
                    if verbose:
                        print(f"  ✗ {ticker}: エラー {e}")
        
        except Exception as e:
            if verbose:
                print(f"Batch error: {e}")
            # フォールバック
            for ticker in batch:
                try:
                    tick = yf.Ticker(ticker)
                    hist = tick.history(period='5d')
                    if not hist.empty:
                        latest_price = hist['Close'].iloc[-1]
                        if min_price <= latest_price <= max_price:
                            filtered.append(ticker)
                            if verbose:
                                print(f"  ✓ {ticker}: ${latest_price:.2f}")
                except:
                    pass
        
        time.sleep(0.5)  # レート制限対策
    
    if verbose:
        print(f"\nフィルタ完了: {len(filtered)}/{total} 銘柄が条件を満たしています")
    
    return filtered


if __name__ == "__main__":
    # テスト実行例
    print("=== 米国株データ取得ツール ===")
    
    # 主要銘柄のみテスト
    test_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    
    print(f"\nテスト: {test_tickers}")
    fetch_and_save_us_tickers(
        tickers=test_tickers,
        batch_size=5,
        period='6mo',
        interval='1d',
        out_dir='data_us',
        verbose=True
    )
    
    print("\n完了! data_us/ フォルダを確認してください")
