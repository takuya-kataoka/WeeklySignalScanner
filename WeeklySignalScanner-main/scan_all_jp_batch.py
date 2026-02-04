#!/usr/bin/env python3
"""
全日本株（1300-9999）を高速スキャン
バッチ処理で効率的に実行
データはスクリプト配置ディレクトリの `data/` 内にあるキャッシュ済みファイルのみを処理（コード >= 1300）
出力はスクリプト配置ディレクトリの `outputs/results` に保存します。
"""
from screener import scan_stocks, scan_stocks_with_cache
import config
from datetime import datetime
import csv
import os
import time
from data_fetcher import load_ticker_from_cache
import yfinance as yf
from pathlib import Path


def scan_range(start, end, output_file, base_dir: Path):
    """指定範囲をスキャン"""
    tickers = [f"{i:04d}.T" for i in range(start, end + 1)]
    # 除外銘柄をフィルタ
    tickers = [t for t in tickers if t not in config.EXCLUDE_TICKERS]

    print(f"スキャン範囲: {start:04d}.T - {end:04d}.T ({len(tickers)}銘柄)")

    results = scan_stocks(
        tickers,
        short_window=10,
        long_window=20,
        period="2y",
        interval="1wk",
        threshold=0.0,
        require_ma52=True,
        require_engulfing=True,
    )

    if results:
        print(f"  ✓ {len(results)}件該当")
        # 追記モードで保存
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for ticker in results:
                price = None
                # キャッシュは config.DATA_DIR を参照
                df = load_ticker_from_cache(ticker, cache_dir=str(config.DATA_DIR))
                try:
                    if df is not None and 'Close' in df.columns and not df['Close'].dropna().empty:
                        price = float(df['Close'].dropna().iloc[-1])
                    else:
                        single = yf.Ticker(ticker).history(period='5d', interval='1d')
                        if single is not None and 'Close' in single.columns and not single['Close'].dropna().empty:
                            price = float(single['Close'].dropna().iloc[-1])
                except Exception:
                    price = None
                writer.writerow([ticker, '' if price is None else f"{price:.2f}"])
    else:
        print(f"  該当なし")

    return len(results)


def main(relaxed_engulfing=False, end_date=None):
    print("=" * 70)
    print("日本株全銘柄スキャン（1300-9999）")
    print("条件: 週足MA52以上 & 陽線包み足")
    if relaxed_engulfing:
        print("  (包み足判定: 緩和モード ON)")
    print("=" * 70)
    print()

    base_dir = Path(__file__).resolve().parent

    # 出力ファイル準備（日本語ファイル名） - スクリプト配置ディレクトリ基準
    outputs_dir = base_dir / 'outputs' / 'results'
    outputs_dir.mkdir(parents=True, exist_ok=True)
    # 出力ファイル名に緩和モードの識別子を付与
    prefix = '全銘柄_MA52_陽線包み'
    if relaxed_engulfing:
        prefix = prefix + '_緩和'
    # 出力ファイル名を作成
    # 単一日指定(as-of)がある場合は: <prefix>_<as-of YYYY-MM-DD>_<作成日時 YYYYmmddHHMMSS>.csv
    if end_date:
        # end_date は文字列 'YYYY-MM-DD' で渡される想定
        created_ts = datetime.now().strftime('%Y%m%d%H%M%S')
        safe = prefix.replace(' ', '_')
        filename = f"{safe}_{end_date}_{created_ts}.csv"
        output_file = str(base_dir / 'outputs' / 'results' / filename)
    else:
        # デフォルトの jp_filename を使う（当日の日付）
        output_file = str(base_dir / config.jp_filename(prefix))

    # ヘッダー書き込み
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ticker', 'price'])

    print(f"結果保存先: {output_file}")
    print()
    print(f"バッチ処理開始（{config.DATA_DIR} 内の銘柄のみ処理）")
    print("=" * 70)
    print()

    start_time = time.time()
    total_found = 0
    # collect (ticker, price) pairs to write sorted later
    found_results = []
    batch_size = 500  # data 内をこの単位で処理

    # collect tickers from local cache 'data' directory if present
    # data_dir は config.DATA_DIR を使う（環境変数で上書き可能）
    data_dir = str(Path(config.DATA_DIR))
    tickers_from_data = []
    if os.path.isdir(data_dir):
        for fn in os.listdir(data_dir):
            # expect files like '6029.T.parquet' -> ticker = '6029.T'
            if fn.endswith('.parquet'):
                ticker = os.path.splitext(fn)[0]
                # only include tickers with numeric code >= 1300
                try:
                    code = int(ticker.replace('.T', ''))
                except Exception:
                    continue
                if code >= 1300:
                    tickers_from_data.append(ticker)
    tickers_from_data = sorted(set(tickers_from_data))
    # apply global exclude list
    tickers = [t for t in tickers_from_data if t not in config.EXCLUDE_TICKERS]

    if not tickers:
        print('data ディレクトリに処理対象のティッカーが見つかりません。')
    else:
        total = len(tickers)
        print(f"処理対象ティッカー数: {total}")
        for idx in range(0, total, batch_size):
            batch = tickers[idx: idx + batch_size]
            print(f"[{idx+1}-{min(idx+batch_size, total)}] ({len(batch)}銘柄)", end=' ')
            try:
                # use cache-aware scanner to avoid re-downloading
                found_list = scan_stocks_with_cache(
                    batch,
                    cache_dir=data_dir,
                    short_window=10,
                    long_window=20,
                    period='2y',
                    interval='1wk',
                    threshold=0.0,
                    require_ma52=True,
                    require_engulfing=True,
                    relaxed_engulfing=relaxed_engulfing,
                    end_date=end_date,
                )
                if found_list:
                    # collect prices and append to results list (don't write per-batch)
                    for ticker in found_list:
                        price = None
                        df = load_ticker_from_cache(ticker, cache_dir=data_dir)
                        try:
                            if relaxed_engulfing:
                                print("  (包み足判定: 緩和モード ON)")
                            print("=" * 70)
                            print()
                            if end_date:
                                print(f"抽出対象日 (as-of): {end_date}")
                            else:
                                print("抽出対象日: 最新")
                                if single is not None and 'Close' in single.columns and not single['Close'].dropna().empty:
                                    price = float(single['Close'].dropna().iloc[-1])
                        except Exception:
                            price = None
                        found_results.append((ticker, price))
                    print(f"  ✓ {len(found_list)}件該当")
                else:
                    print("  該当なし")
            except Exception as e:
                print(f"  エラー: {e}")
            # progress
            progress = (idx + len(batch)) / total * 100
            elapsed = time.time() - start_time
            print(f"  進捗: {progress:.1f}% (経過時間: {elapsed/60:.1f}分)")

    elapsed_total = time.time() - start_time

    # After processing all batches, write sorted results by price (ascending)
    # price None values will be placed at the end
    def _price_key(item):
        # item: (ticker, price)
        # price may be None; place None values at the end by returning a tuple
        ticker, price = item
        if price is None:
            return (1, float('inf'))
        try:
            return (0, float(price))
        except Exception:
            return (1, float('inf'))

    found_results_sorted = sorted(found_results, key=_price_key)
    # overwrite output file with header + sorted rows
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ticker', 'price'])
        for ticker, price in found_results_sorted:
            writer.writerow([ticker, '' if price is None else f"{price:.2f}"])

    total_found = len(found_results_sorted)
    
    print()
    print("=" * 70)
    print("スキャン完了")
    print("=" * 70)
    print()
    print(f"総検出銘柄数: {total_found}件")
    print(f"処理時間: {elapsed_total/60:.1f}分")
    print(f"結果: {output_file}")
    print()
    
    # 結果を読み込んで表示
    if total_found > 0:
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # ヘッダーをスキップ
            tickers = [row[0] for row in reader]
        
        print("該当銘柄:")
        for i, ticker in enumerate(tickers, 1):
            print(f"  {i}. {ticker}")
    else:
        print("条件を満たす銘柄はありませんでした。")


if __name__ == "__main__":
    main()
