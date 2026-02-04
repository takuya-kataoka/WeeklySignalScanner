import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import glob
import os
import yfinance as yf
from pathlib import Path
import math
import datetime
import re

st.set_page_config(page_title="週足スクリーナー", layout="wide")

# バージョン表示: ルートの VERSION ファイルを参照して動的に表示する
version = "1.00"
try:
    # base_dir はこのファイルのディレクトリ（WeeklySignalScanner-main）
    repo_root = base_dir.parent
    version_path = repo_root / 'VERSION'
    if version_path.exists():
        version = version_path.read_text(encoding='utf-8').strip()
except Exception:
    pass

st.title(f"📈 週足スクリーナー - MA52 & 陽線包み足 ver1.02")

# ベースディレクトリを明示（スクリプトの配置ディレクトリ基準にする）
base_dir = Path(__file__).resolve().parent

# デバイス選択: PC / Mobile（UI のサイズ調整に使う）
device_mode = st.sidebar.selectbox('表示デバイス', ['PC', 'Mobile'], index=0)
IS_MOBILE = (device_mode == 'Mobile')

# レスポンシブCSSを挿入（モバイル向けにフォントやパディングを調整）
if IS_MOBILE:
    st.markdown(
        """
        <style>
        /* Mobile adjustments */
        .stApp .block-container { padding: 0.6rem 0.6rem !important; max-width: 100% !important; }
        .stApp h1 { font-size: 1.4rem !important; }
        .stApp h2, .stApp h3 { font-size: 1.05rem !important; }
        .stApp p, .stApp label, .stApp .stText { font-size: 1.0rem !important; }
        .stButton>button { padding: 0.4rem 0.8rem !important; font-size: 1.0rem !important; }
        .css-1d391kg { margin: 0 !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        """
        <style>
        /* Desktop: constrain content width for readability */
        .stApp .block-container { max-width: 1200px !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

# 結果ファイルを選択（任意の CSV を選べるように変更）
results_dir = base_dir / 'outputs' / 'results'
# 最新の更新日時が上に来るように modification time (mtime) でソート
all_files = sorted(results_dir.glob('*.csv'), key=lambda p: p.stat().st_mtime, reverse=True) if results_dir.exists() else []

if not all_files:
    st.error("結果ファイルが見つかりません")
    st.stop()

# デフォルトは全銘柄の昇順ソート版があればそれを優先して選択
default_index = 0
for i, f in enumerate(all_files):
    name = f.name
    if '全銘柄' in name and 'sorted_asc' in name:
        default_index = i
        break

selected_file = st.sidebar.selectbox(
    "結果ファイルを選択",
    all_files,
    index=default_index,
    format_func=lambda x: x.name
)

# 管理パネル: データのダウンロード / 抽出ファイル作成 / 予想ページ起動
with st.sidebar.expander("管理: データ取得・スキャン・予想", expanded=False):
    st.write("データのダウンロードやスキャン、予想ページ起動ができます")

    fetch_period = st.text_input('fetch period (yfinance)', value='1y')
    fetch_interval = st.text_input('fetch interval', value='1d')
    fetch_batch = st.number_input('batch size', min_value=1, value=200)
    fetch_sleep = st.number_input('sleep between batches (s)', min_value=0.0, value=1.0, step=0.1)
    fetch_start = st.number_input('start code (4-digit)', min_value=0, value=1300)
    fetch_end = st.number_input('end code (4-digit)', min_value=0, value=9999)

    # 手動ティッカー入力（カンマ区切り）
    manual_tickers = st.text_input('手動ティッカー (カンマ区切り、例: 7201,7202 または 7201.T,7202.T)', value='')

    # 除外リストを無視して取得するか
    allow_excluded = st.checkbox('除外リストを無視して取得 (EXCLUDED を含める)', value=False)

    # ダウンロード動作: data/ の既存銘柄のみ、差分、または範囲内全件を選択
    fetch_mode = st.selectbox('ダウンロード対象', [
        'data に存在する銘柄のみ取得（既存銘柄を再取得）',
        '今日の日付が無いものだけ取得（差分更新）',
        'すべての銘柄を取得（範囲内全件）'
    ])
    if st.button('データをダウンロード'):
        import data_fetcher
        from data_fetcher import load_ticker_from_cache, fetch_and_save_list
        import pandas as _pd

        # 範囲（start/end）は共通
        start_code = int(fetch_start)
        end_code = int(fetch_end)

        # 候補の構築
        candidates = []
        data_dir = 'data'

        # 優先: 手動ティッカーが入力されていればそれを使う
        if manual_tickers and manual_tickers.strip():
            parts = [p.strip() for p in re.split('[,\n;]+', manual_tickers) if p.strip()]
            parsed = []
            for p in parts:
                token = p
                # 数値のみなら 4 桁ゼロ埋めして .T を付与
                if re.fullmatch(r"\d{1,4}", token):
                    token = f"{int(token):04d}.T"
                else:
                    if not token.upper().endswith('.T'):
                        token = token.upper()
                parsed.append(token)
            candidates = parsed
        else:
            if fetch_mode.startswith('すべての銘柄'):
                # 範囲内の全銘柄を対象にする
                candidates = [f"{i:04d}.T" for i in range(start_code, end_code + 1)]
                if not allow_excluded:
                    try:
                        excluded = getattr(data_fetcher, 'EXCLUDED_TICKERS', set())
                        candidates = [t for t in candidates if t not in excluded]
                    except Exception:
                        pass
            else:
                # data/ に存在する銘柄のみを候補とする
                tickers_from_data = []
                if os.path.isdir(data_dir):
                    for fn in os.listdir(data_dir):
                        if fn.endswith('.parquet'):
                            ticker = os.path.splitext(fn)[0]
                            try:
                                code = int(ticker.replace('.T', ''))
                            except Exception:
                                continue
                            tickers_from_data.append(ticker)
                tickers_from_data = sorted(set(tickers_from_data))
                candidates = [t for t in tickers_from_data if start_code <= int(t.replace('.T','')) <= end_code]

        if not candidates:
            st.info('取得対象の銘柄が見つかりません（範囲や data/ を確認してください）')
            st.stop()

        targets = []
        today = _pd.Timestamp.today().normalize()
        if fetch_mode.startswith('data に存在する'):
            targets = candidates
        elif fetch_mode.startswith('今日の日付が無い'):
            for t in candidates:
                df = load_ticker_from_cache(t, cache_dir='data')
                if df is None:
                    targets.append(t)
                    continue
                try:
                    last = _pd.to_datetime(df.index.max()).normalize()
                    if last < today:
                        targets.append(t)
                except Exception:
                    targets.append(t)
        else:
            # すべての銘柄モード
            targets = candidates

        if not targets:
            st.info('取得対象はありません（すでに最新）')
        else:
            with st.spinner(f'取得中... {len(targets)} 銘柄'):
                try:
                    # allow_excluded を fetch に渡す
                    fetch_and_save_list(targets, batch_size=int(fetch_batch), period=fetch_period, interval=fetch_interval, out_dir='data', retry_count=1, sleep_between_batches=float(fetch_sleep), allow_excluded=allow_excluded, verbose=True)
                    st.success('データダウンロード完了')
                except Exception as e:
                    st.error(f'ダウンロード中にエラー: {e}')

    # 包み足判定を緩和するか（チェック時のみ緩和） - スキャンボタン近くに配置
    relax_engulfing = st.checkbox('包み足判定を緩和する（チェック時のみ有効）', value=False)

    # 抽出対象日指定オプション: 指定日での判定を行う（デフォルトは当日）
    use_as_of_date = st.checkbox('抽出対象日を指定する（指定が無ければ最新版）', value=False)
    as_of_date = None
    if use_as_of_date:
        today = datetime.date.today()
        as_of_date = st.date_input('抽出対象日', value=today)

    if st.button('抽出ファイルを作成（スキャン）'):
        # 実行には時間がかかるため実行中インジケータを表示
        import scan_all_jp_batch
        with st.spinner('スキャン中... data/ のキャッシュを使って処理します'):
            try:
                # pass as-of date when user enabled it
                if use_as_of_date and as_of_date:
                    scan_all_jp_batch.main(relaxed_engulfing=relax_engulfing, as_of_date=str(as_of_date))
                else:
                    scan_all_jp_batch.main(relaxed_engulfing=relax_engulfing)
                st.success('スキャン完了: outputs/results を確認してください')
                # Streamlit Cloud 上でスキャン結果をリポジトリにコミットしてプッシュする処理
                try:
                    import subprocess, datetime

                    st.sidebar.info('スキャン完了 — Git にコミットを試行します')

                    repo_root = base_dir.parent
                    # git のユーザー情報を設定（必要に応じて環境側で上書きしてください）
                    subprocess.run(['git', '-C', str(repo_root), 'config', 'user.email', 'streamlit@example.com'], check=False)
                    subprocess.run(['git', '-C', str(repo_root), 'config', 'user.name', 'StreamlitAutoCommit'], check=False)

                    # スキャン結果ディレクトリをステージ（CSV 等すべて）
                    try:
                        # add each file under results with -f to override .gitignore if needed
                        added_any = False
                        for p in sorted(results_dir.rglob('*')):
                            if p.is_file():
                                relp = os.path.relpath(str(p), start=str(repo_root))
                                add_proc = subprocess.run(['git', '-C', str(repo_root), 'add', '-f', relp], capture_output=True, text=True)
                                st.sidebar.info(f'git add {relp} -> returncode={add_proc.returncode} stderr:{add_proc.stderr}')
                                if add_proc.returncode == 0:
                                    added_any = True
                        if not added_any:
                            st.sidebar.info('outputs/results 内にステージ可能なファイルが見つかりませんでした（.gitignore を確認してください）')
                    except Exception as e:
                        st.sidebar.error(f'git add 実行中に例外: {e}')

                    # commit 前に VERSION を自動バンプしてステージする
                    try:
                        bump_script = repo_root / 'scripts' / 'bump_version.py'
                        if bump_script.exists():
                            subprocess.run(['python3', str(bump_script)], check=False)
                            subprocess.run(['git', '-C', str(repo_root), 'add', 'VERSION'], check=False)
                    except Exception:
                        pass

                    # バージョン情報取得（あれば）
                    version_str = ''
                    try:
                        vpath = repo_root / 'VERSION'
                        if vpath.exists():
                            version_str = vpath.read_text(encoding='utf-8').strip()
                    except Exception:
                        version_str = ''

                    commit_msg = f"chore(scan): add scan results{' ver'+version_str if version_str else ''} {datetime.datetime.utcnow().isoformat()}"
                    commit_proc = subprocess.run(['git', '-C', str(repo_root), 'commit', '-m', commit_msg], capture_output=True, text=True)
                    st.sidebar.info(f'git commit returncode={commit_proc.returncode}\nstdout:{commit_proc.stdout}\nstderr:{commit_proc.stderr}')
                    if commit_proc.returncode != 0:
                        # 変更が無ければコミットは失敗（何も to commit）することがあるので情報を出す
                        if 'nothing to commit' in (commit_proc.stdout + commit_proc.stderr).lower():
                            st.sidebar.info('コミットする変更はありませんでした（既に最新の可能性）')
                        else:
                            st.sidebar.error('git commit が失敗しました。サイドバーのログを確認してください。')
                    else:
                        # プッシュ
                        try:
                            # Try to use token from environment or Streamlit secrets if available
                            token = os.environ.get('GITHUB_TOKEN')
                            try:
                                # Streamlit secrets (if deployed with secrets)
                                if not token:
                                    token = st.secrets.get('GITHUB_TOKEN') if hasattr(st, 'secrets') and 'GITHUB_TOKEN' in st.secrets else None
                            except Exception:
                                pass

                            if token:
                                # get origin URL and construct authenticated push URL without logging token
                                rem = subprocess.run(['git', '-C', str(repo_root), 'remote', 'get-url', 'origin'], capture_output=True, text=True)
                                origin_url = rem.stdout.strip()
                                if origin_url.startswith('https://'):
                                    auth_url = origin_url.replace('https://', f'https://{token}@')
                                else:
                                    auth_url = origin_url
                                push_proc = subprocess.run(['git', '-C', str(repo_root), 'push', auth_url, 'main'], capture_output=True, text=True, timeout=120)
                            else:
                                push_proc = subprocess.run(['git', '-C', str(repo_root), 'push', 'origin', 'main'], capture_output=True, text=True, timeout=120)

                            st.sidebar.info(f'git push returncode={push_proc.returncode}\nstdout:{push_proc.stdout}\nstderr:{push_proc.stderr}')
                            if push_proc.returncode == 0:
                                st.sidebar.success('スキャン結果を origin/main にコミット＆プッシュしました')
                            else:
                                st.sidebar.error(f'git push に失敗しました: {push_proc.stderr}\n認証情報が設定されているか確認してください。')
                        except Exception as e:
                            st.sidebar.error(f'git push 実行中に例外: {e}')
                except Exception as e:
                    st.sidebar.error(f'自動コミット中に例外が発生しました: {e}')
            except Exception as e:
                st.error(f'スキャン中にエラー: {e}')

    # ファイル削除 UI
    with st.expander('ファイル管理: outputs/results の削除', expanded=False):
        try:
            repo_root = base_dir.parent
            files = sorted(results_dir.glob('*.csv'), key=lambda p: p.stat().st_mtime, reverse=True) if results_dir.exists() else []
            file_names = [p.name for p in files]
            to_delete = st.multiselect('削除するファイルを選択', file_names)
            if to_delete:
                if st.button('選択ファイルを削除'):
                    import subprocess, datetime
                    st.sidebar.info('選択ファイルを削除します...')
                    removed = []
                    for name in to_delete:
                        p = results_dir / name
                        relp = os.path.relpath(str(p), start=str(repo_root))
                        try:
                            # Try git rm -f to stage deletion for tracked files
                            rm_proc = subprocess.run(['git', '-C', str(repo_root), 'rm', '-f', relp], capture_output=True, text=True)
                            st.sidebar.info(f'git rm {name} -> returncode={rm_proc.returncode} stderr:{rm_proc.stderr}')
                            if rm_proc.returncode != 0:
                                # fallback: remove file and stage
                                if p.exists():
                                    p.unlink()
                                add_proc = subprocess.run(['git', '-C', str(repo_root), 'add', '-A'], capture_output=True, text=True)
                                st.sidebar.info(f'git add -A -> returncode={add_proc.returncode} stderr:{add_proc.stderr}')
                            removed.append(name)
                        except Exception as e:
                            st.sidebar.error(f'ファイル削除中に例外: {e}')

                    if removed:
                        # bump VERSION and stage it
                        try:
                            bump_script = repo_root / 'scripts' / 'bump_version.py'
                            if bump_script.exists():
                                subprocess.run(['python3', str(bump_script)], check=False)
                                subprocess.run(['git', '-C', str(repo_root), 'add', 'VERSION'], check=False)
                        except Exception:
                            pass

                        # Commit
                        version_str = ''
                        try:
                            vpath = repo_root / 'VERSION'
                            if vpath.exists():
                                version_str = vpath.read_text(encoding='utf-8').strip()
                        except Exception:
                            version_str = ''

                        commit_msg = f"chore(clean): remove results {','.join(removed)}{' ver'+version_str if version_str else ''} {datetime.datetime.utcnow().isoformat()}"
                        commit_proc = subprocess.run(['git', '-C', str(repo_root), 'commit', '-m', commit_msg], capture_output=True, text=True)
                        st.sidebar.info(f'git commit returncode={commit_proc.returncode}\nstdout:{commit_proc.stdout}\nstderr:{commit_proc.stderr}')
                        # Push (use token if available)
                        try:
                            token = os.environ.get('GITHUB_TOKEN')
                            try:
                                if not token:
                                    token = st.secrets.get('GITHUB_TOKEN') if hasattr(st, 'secrets') and 'GITHUB_TOKEN' in st.secrets else None
                            except Exception:
                                pass

                            if token:
                                rem = subprocess.run(['git', '-C', str(repo_root), 'remote', 'get-url', 'origin'], capture_output=True, text=True)
                                origin_url = rem.stdout.strip()
                                if origin_url.startswith('https://'):
                                    auth_url = origin_url.replace('https://', f'https://{token}@')
                                else:
                                    auth_url = origin_url
                                push_proc = subprocess.run(['git', '-C', str(repo_root), 'push', auth_url, 'main'], capture_output=True, text=True, timeout=120)
                            else:
                                push_proc = subprocess.run(['git', '-C', str(repo_root), 'push', 'origin', 'main'], capture_output=True, text=True, timeout=120)

                            st.sidebar.info(f'git push returncode={push_proc.returncode}\nstdout:{push_proc.stdout}\nstderr:{push_proc.stderr}')
                            if push_proc.returncode == 0:
                                st.sidebar.success('選択ファイルの削除をコミット＆プッシュしました')
                            else:
                                st.sidebar.error('git push に失敗しました。認証情報を確認してください。')
                        except Exception as e:
                            st.sidebar.error(f'git push 実行中に例外: {e}')
        except Exception:
            st.error('ファイル一覧の取得に失敗しました')

    st.write('---')
    st.write('予想ページ起動（外部Streamlitを別ポートで起動）')
    app_options = {
        '既存: app_predict.py': 'app_predict.py',
        '新規: 血統予想 app (streamlit_horse_app.py)': 'streamlit_horse_app.py'
    }
    chosen_label = st.selectbox('起動するアプリを選択', list(app_options.keys()))
    chosen_app = app_options[chosen_label]
    chosen_port = st.number_input('起動ポート', min_value=1024, max_value=65535, value=8502)
    if st.button('選択アプリを起動'):
        import subprocess, os
        out_log = str(base_dir / 'outputs' / f'streamlit_{chosen_port}.log')
        os.makedirs(str(base_dir / 'outputs'), exist_ok=True)
        streamlit_bin = os.path.abspath('/workspaces/WeeklySignalScanner-main/.venv/bin/streamlit')
        app_path = os.path.abspath(base_dir / chosen_app)
        cmd = f"nohup env STREAMLIT_BROWSER_GUESSING=false STREAMLIT_DISABLE_TELEMETRY=1 {streamlit_bin} run {app_path} --server.port {chosen_port} --server.headless true > {out_log} 2>&1 &"
        try:
            subprocess.Popen(cmd, shell=True, cwd=os.getcwd())
            st.info(f'起動コマンドを送信しました: {chosen_app} -> http://localhost:{chosen_port}')
            st.write('Local URL:', f'http://localhost:{chosen_port}')
            st.write(f'ログ: {out_log}')
        except Exception as e:
            st.error(f'予想ページ起動に失敗しました: {e}')

# データ読み込み（先頭に retrieved_at メタ行がある場合はスキップ）
def read_maybe_timestampped_csv(path):
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            first = fh.readline()
        if first.startswith('retrieved_at,'):
            return pd.read_csv(path, skiprows=1)
        return pd.read_csv(path)
    except Exception:
        return pd.read_csv(path)

df = read_maybe_timestampped_csv(selected_file)

# 表示: 結果ファイルと data ディレクトリの最終更新時刻をサイドバーに表示
try:
    sel_mtime = datetime.datetime.fromtimestamp(Path(str(selected_file)).stat().st_mtime)
    sel_mtime_str = sel_mtime.strftime("%Y-%m-%d %H:%M:%S")
except Exception:
    sel_mtime_str = "-"

data_dir_path = base_dir.parent / 'data'
data_latest_str = "-"
if data_dir_path.exists():
    max_m = None
    for p in data_dir_path.rglob('*'):
        if p.is_file():
            try:
                m = p.stat().st_mtime
            except Exception:
                continue
            if max_m is None or m > max_m:
                max_m = m
    if max_m:
        data_latest_str = datetime.datetime.fromtimestamp(max_m).strftime("%Y-%m-%d %H:%M:%S")

st.sidebar.markdown(f"**データ情報**\n- 結果ファイル更新: {sel_mtime_str}\n- data 最終更新: {data_latest_str}")

# 追加: outputs/results の中から最新で価格列（'current_price' または 'price'）を持つCSVを自動検出して読み込み
price_map = {}
price_file = None
candidates = sorted([str(p) for p in (results_dir.glob('*.csv'))], key=os.path.getmtime, reverse=True) if results_dir.exists() else []
for p in candidates:
    try:
        # read first line to detect timestamp metadata
        with open(p, 'r', encoding='utf-8') as fh:
            first = fh.readline()
        if first.startswith('retrieved_at,'):
            pf = pd.read_csv(p, skiprows=1)
        else:
            pf = pd.read_csv(p)

        if 'current_price' in pf.columns or 'price' in pf.columns:
            price_file = p
            # build price_map and stop at the first (newest) match
            if 'current_price' in pf.columns and 'ticker' in pf.columns:
                price_map = pd.Series(pf['current_price'].values, index=pf['ticker'].astype(str)).to_dict()
            elif 'price' in pf.columns and 'ticker' in pf.columns:
                price_map = pd.Series(pf['price'].values, index=pf['ticker'].astype(str)).to_dict()
            break
    except Exception:
        continue

# 価格でソート（結果ファイルに price 列または別途作成した price_map がある場合）
if 'price' in df.columns:
    df = df.sort_values('price').reset_index(drop=True)
elif price_map:
    # マップに基づいて price 列を作りソート
    df = df.copy()
    df['price'] = df['ticker'].astype(str).map(price_map)
    df = df.sort_values('price').reset_index(drop=True)

st.sidebar.metric("検出銘柄数", len(df))

# 銘柄選択
if 'ticker' not in df.columns:
    st.error("ticker列が見つかりません")
    st.stop()

ticker_list = df['ticker'].tolist()

# 表示モード選択
display_mode = st.sidebar.radio("表示モード", ["単一銘柄", "10銘柄一覧"])

if display_mode == "単一銘柄":
    selected_ticker = st.sidebar.selectbox("銘柄を選択", ticker_list)
    selected_tickers = [selected_ticker]
else:
    # 10銘柄ずつページング
    total_pages = math.ceil(len(ticker_list) / 10)
    page = st.sidebar.number_input("ページ", min_value=1, max_value=total_pages, value=1, step=1)
    start_idx = (page - 1) * 10
    end_idx = min(start_idx + 10, len(ticker_list))
    selected_tickers = ticker_list[start_idx:end_idx]
    st.sidebar.info(f"ページ {page}/{total_pages} (銘柄 {start_idx+1}〜{end_idx})")
    
    # 2列レイアウトで表示
    cols_per_row = 2

# データ取得
@st.cache_data(ttl=3600)
def fetch_data(ticker):
    try:
        data = yf.Ticker(ticker).history(period='2y', interval='1wk')
        if data.empty:
            return None
        return data
    except Exception as e:
        return None

# 選択された銘柄に対してチャート表示
if display_mode == "10銘柄一覧":
    # 2列グリッドレイアウト
    for i in range(0, len(selected_tickers), cols_per_row):
        cols = st.columns(cols_per_row)
        for j, col in enumerate(cols):
            idx = i + j
            if idx >= len(selected_tickers):
                break
            ticker = selected_tickers[idx]
            
            with col:
                data = fetch_data(ticker)
                
                if data is None:
                    st.warning(f"{ticker}: データ取得失敗")
                    continue
                
                # メトリクス表示（コンパクト）
                # まず price_map に価格があればそれを優先して表示（ページ切替で値が固定される問題を回避）
                latest_close = price_map.get(str(ticker)) if price_map else None
                if latest_close is None:
                    latest_close = data['Close'].iloc[-1]
                ma52 = data['Close'].rolling(52).mean().iloc[-1]


                st.markdown(f"**{ticker}**  ¥{latest_close:,.0f}")
                
                # チャート作成（小さめサイズ）
                fig = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.05,
                    row_heights=[0.75, 0.25]
                )
                
                # ローソク足
                fig.add_trace(
                    go.Candlestick(
                        x=data.index,
                        open=data['Open'],
                        high=data['High'],
                        low=data['Low'],
                        close=data['Close'],
                        name='価格',
                        increasing_line_color='red',
                        decreasing_line_color='blue',
                        showlegend=False
                    ),
                    row=1, col=1
                )
                
                # MA52
                fig.add_trace(
                    go.Scatter(
                        x=data.index,
                        y=data['Close'].rolling(52).mean(),
                        name='MA52',
                        line=dict(color='orange', width=1),
                        showlegend=False
                    ),
                    row=1, col=1
                )
                
                # 出来高
                colors = ['red' if data['Close'].iloc[k] >= data['Open'].iloc[k] else 'blue' 
                          for k in range(len(data))]
                
                fig.add_trace(
                    go.Bar(
                        x=data.index,
                        y=data['Volume'],
                        marker_color=colors,
                        showlegend=False
                    ),
                    row=2, col=1
                )
                
                # レイアウト調整（コンパクト）
                fig.update_layout(
                    height=300,
                    margin=dict(l=30, r=10, t=20, b=20),
                    xaxis_rangeslider_visible=False,
                    hovermode='x unified',
                    template='plotly_white',
                    showlegend=False,
                    font=dict(size=8)
                )
                
                fig.update_yaxes(title_text="", row=1, col=1)
                fig.update_yaxes(title_text="", row=2, col=1)
                fig.update_xaxes(showticklabels=False, row=1, col=1)
                fig.update_xaxes(showticklabels=False, row=2, col=1)
                
                st.plotly_chart(fig, use_container_width=True, key=f"chart_grid_{ticker}")

else:
    # 単一銘柄モード
    for ticker in selected_tickers:
        data = fetch_data(ticker)
        
        if data is None:
            st.warning(f"{ticker}: データを取得できませんでした")
            continue
        
        # 区切り線
        st.markdown("---")
        
        # メトリクス表示
        col1, col2, col3, col4, col5 = st.columns(5)
        
        # 単一銘柄モードでも price_map の値を優先する
        latest_close = price_map.get(str(ticker)) if price_map else None
        if latest_close is None:
            latest_close = data['Close'].iloc[-1]
        latest_volume = data['Volume'].iloc[-1]
        ma52 = data['Close'].rolling(52).mean().iloc[-1]
        change_pct = ((latest_close - data['Close'].iloc[-2]) / data['Close'].iloc[-2] * 100) if len(data) > 1 else 0
        
        with col1:
            st.metric("銘柄", ticker)
        with col2:
            st.metric("株価", f"¥{latest_close:,.2f}", f"{change_pct:+.2f}%")
        with col3:
            st.metric("出来高", f"{latest_volume:,.0f}")
        with col4:
            st.metric("52週MA", f"¥{ma52:,.2f}")
        with col5:
            ma_diff_pct = ((latest_close - ma52) / ma52 * 100)
            st.metric("MA52比", f"{ma_diff_pct:+.2f}%")
        # キャッシュされたデータの最終日または更新時刻を表示
        try:
            cache_path = base_dir.parent / 'data' / f"{ticker}.parquet"
            cache_info = None
            if cache_path.exists():
                try:
                    cdf = pd.read_parquet(cache_path)
                    # インデックスに日付がある場合は最終日を表示
                    if hasattr(cdf.index, 'max'):
                        idxmax = cdf.index.max()
                        cache_info = f"キャッシュ最終日: {pd.to_datetime(idxmax).date()}"
                except Exception:
                    cache_info = f"キャッシュ最終更新: {datetime.datetime.fromtimestamp(cache_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')}"
            if cache_info:
                st.caption(cache_info)
        except Exception:
            pass
        
        # チャート作成
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.7, 0.3],
            subplot_titles=(f'{ticker} 週足チャート', '出来高')
        )
        
        # ローソク足
        fig.add_trace(
            go.Candlestick(
                x=data.index,
                open=data['Open'],
                high=data['High'],
                low=data['Low'],
                close=data['Close'],
                name='価格',
                increasing_line_color='red',
                decreasing_line_color='blue'
            ),
            row=1, col=1
        )
        
        # MA52
        fig.add_trace(
            go.Scatter(
                x=data.index,
                y=data['Close'].rolling(52).mean(),
                name='MA52',
                line=dict(color='orange', width=2)
            ),
            row=1, col=1
        )
        
        # 出来高
        colors = ['red' if data['Close'].iloc[i] >= data['Open'].iloc[i] else 'blue' 
                  for i in range(len(data))]
        
        fig.add_trace(
            go.Bar(
                x=data.index,
                y=data['Volume'],
                name='出来高',
                marker_color=colors,
                showlegend=False
            ),
            row=2, col=1
        )
        
        # レイアウト調整
        fig.update_layout(
            height=600,
            xaxis_rangeslider_visible=False,
            hovermode='x unified',
            template='plotly_white',
            showlegend=True
        )
        
        fig.update_yaxes(title_text="株価 (¥)", row=1, col=1)
        fig.update_yaxes(title_text="出来高", row=2, col=1)
        fig.update_xaxes(title_text="日付", row=2, col=1)
        
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{ticker}")
        
 # test