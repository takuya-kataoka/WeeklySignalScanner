"""共通設定ファイル
将来的に除外銘柄やファイル名テンプレートなどをここで管理します。
"""
import os
from datetime import datetime
from pathlib import Path

# デフォルトのデータ保存先（環境変数 `DATA_DIR` で上書き可能）
# config.py が置かれている場所は `.../WeeklySignalScanner-main/WeeklySignalScanner-main` のため
# ワークスペースルートの `data/` をデフォルトにするため parent.parent を使う
_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = os.environ.get('DATA_DIR', str(_ROOT / 'data'))

# 除外する銘柄リスト（ユーザ指定）
# ユーザからの入力を正規化して .T を付与して扱う
RAW_EXCLUDE = [
    "1328",
    "1477",
    "1541",
    "1560.T",
    "1620.T",
    "2070",
    "2071",
    "2089",
    "2235",
]


def _normalize(ticker):
    t = str(ticker).strip()
    if t.endswith('.T'):
        return t
    # if numeric, pad to 4 digits
    if t.isdigit():
        return f"{int(t):04d}.T"
    return t


EXCLUDE_TICKERS = set(_normalize(t) for t in RAW_EXCLUDE)

# 出力ファイル名テンプレート（日本語）
# 例: 全銘柄スキャン結果 -> 'outputs/results/全銘柄_MA52_陽線包み_2025-12-12.csv'
DATE_FORMAT = '%Y-%m-%d'

def jp_filename(prefix: str, date: datetime = None):
    """prefix は日本語の説明文字列（例: '全銘柄_MA52_陽線包み'）
    戻り値: outputs/results/{prefix}_{YYYY-MM-DD}.csv
    """
    if date is None:
        date = datetime.now()
    date_str = date.strftime(DATE_FORMAT)
    # 自動的に冗長なキーワードを取り除く（ファイル名が長くなりすぎるため）
    safe_prefix = str(prefix)
    # remove common scan keywords
    for token in ['MA52', 'M52', '陽線包み', '陽線', '包み']:
        safe_prefix = safe_prefix.replace(token, '')
    # collapse multiple underscores and spaces
    safe = safe_prefix.replace(' ', '_')
    while '__' in safe:
        safe = safe.replace('__', '_')
    safe = safe.strip('_')
    out_path = f"outputs/results/{safe}_{date_str}.csv"
    # ファイル作成時にどのファイル名が使われたかを表示（ログ出力）
    try:
        print(f"[WSS] output filename: {out_path}")
    except Exception:
        pass
    return out_path


if __name__ == '__main__':
    print('EXCLUDE_TICKERS:', EXCLUDE_TICKERS)
