# WeeklySignalScanner

簡単な日本株ウィークリースクリーナー。

Usage
-----

1. 仮想環境を作成して依存をインストール:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

2. `app.py` を実行:

```bash
python app.py
```

パラメータ
---------

`screener.scan_stocks` は以下のパラメータを受け取ります:

- `short_window` (int): 短期移動平均の窓（例: 10）
- `long_window` (int): 長期移動平均の窓（例: 20）
- `period` (str): yfinance に渡すデータ取得期間（例: "2y"）
- `interval` (str): データ間隔（例: "1wk"）
- `threshold` (float): 判定の閾値（比率）。最新の短期MAが長期MAより `threshold` 以上上回る必要がある（例: 0.01 = 1%）
- `require_ma52` (bool): 最新終値が週足MA52以上であることを要求する（デフォルト: True）
- `require_engulfing` (bool): 直近の陽線包み足を要求する（デフォルト: True）

`app.py` からこれらの値を変更して実行できます。

Universe（全銘柄から価格フィルタ）モード
---------------------------------

`app.py` 内の `universe_mode` を `True` にすると、`1000`〜`9999` の4桁コードに `.T` を付けた銘柄群を自動生成し、
終値が `max_price` 以下の銘柄だけを対象にしてスキャンします。

注意点:
- 大量の銘柄を一度にダウンロードするため時間がかかります。
- Yahoo Finance 側の制限や無効なティッカー（上場廃止など）でダウンロード失敗が発生します。
- 実行前に `app.py` の `batch_size` 相当の値（`generate_jp_tickers_under_price` の引数）を調整してください。

テスト実行（例: 小さい範囲で試す）:

```bash
source .venv/bin/activate
python -c "from screener import generate_jp_tickers_under_price; print(generate_jp_tickers_under_price(max_price=1000, start=1300, end=1400, batch_size=50))"
```

本番で全コードを走らせる場合は `start=1000, end=9999` を指定しますが、時間がかかる点に注意してください。

キャッシュ（Parquet）を使った高速スキャン
----------------------------------

作業の流れ:

1. 一度だけローカルに全銘柄（または必要な範囲）のデータを取得して保存します（デフォルトは期間 6ヶ月）。
	- 例（小範囲のテスト）:

```bash
source .venv/bin/activate
python -c "from data_fetcher import fetch_and_save_tickers; fetch_and_save_tickers(start=7200, end=7210, batch_size=5, period='6mo', interval='1d', out_dir='data', verbose=True)"
```

2. 保存した Parquet を参照してスクリーニングします（ネットアクセス不要、速い）:

```bash
source .venv/bin/activate
python -c "from screener import scan_stocks_with_cache; tickers=['7201.T','7205.T','7208.T']; print(scan_stocks_with_cache(tickers, cache_dir='data'))"
```

3. 全銘柄（1000–9999）をキャッシュする例（時間がかかります）:

```bash
source .venv/bin/activate
python -c "from data_fetcher import fetch_and_save_tickers; fetch_and_save_tickers(start=1000, end=9999, batch_size=200, period='6mo', interval='1d', out_dir='data', verbose=True)"
```

注意:
- Parquet 格納先は `data/`（デフォルト）です。各ファイルが `data/7203.T.parquet` のように保存されます。
- フル取得は数十分〜数時間かかる場合があります。`batch_size` や `sleep_between_batches` を小さく/大きく調整してください。
- 取得失敗したティッカーはログに出ます。必要なら再実行で補完してください。

判定基準の変更
----------------

現在の判定基準は以下です:

- 週足で「陽線包み足（bullish engulfing）」が発生していること（`require_engulfing=True` の場合）
- 最新週の終値が週足の52週移動平均（MA52）以上であること（`require_ma52=True` の場合）

技術的には、直近2週のローソク足を見て、前週が陰線／当週が陽線で、当週の実体が前週の実体を包んでいる（当週の始値 <= 前週の終値 かつ 当週の終値 >= 前週の始値）場合を「陽線包み足」と判定します。その上で最新終値 >= MA52 の条件を満たす銘柄が出力されます。

除外する銘柄（取得・スキャンとも常にスキップ）
--------------------------------------------

`1326.T`, `2012.T`, `2013.T`, `1325.T`, `2250.T`, `1656.T`

Streamlitアプリで結果を表示
---------------------------

週足チャート、株価、出来高を表示するWebアプリを起動:

```bash
source .venv/bin/activate
streamlit run app_streamlit.py
```

ブラウザで自動的に開きます（通常 http://localhost:8501）。

機能:
- 結果ファイル（`ma52_engulfing_*.csv`）から銘柄を選択表示
- 週足ローソク足チャート + MA52ライン
- 出来高バーチャート
- 直近20週のデータテーブル
- 全検出銘柄リスト