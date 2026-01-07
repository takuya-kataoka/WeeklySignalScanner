# Dev Container Configuration

このプロジェクトは VS Code の Dev Container に対応しています。

## 使い方

1. VS Code で Docker がインストールされていることを確認
2. VS Code で "Remote - Containers" 拡張機能をインストール
3. プロジェクトを開き、`F1` → "Dev Containers: Reopen in Container" を選択

## 含まれる機能

- Python 3.12
- Git
- GitHub CLI
- VS Code 拡張機能:
  - Python
  - Pylance
  - Jupyter
  - Black Formatter
- 自動ポート転送: 8501 (Streamlit)

## 自動セットアップ

コンテナ作成時に自動的に `requirements.txt` からパッケージがインストールされます。

## Streamlit アプリの起動

```bash
streamlit run app_streamlit.py
```

ポート 8501 が自動的に転送されます。
