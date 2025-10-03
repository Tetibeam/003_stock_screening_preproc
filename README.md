# 株式銘柄のスクリーニング前処理ツール

## プロジェクト概要
このプロジェクトは、CSVファイルを読み込み、欠損値・異常値を処理し、1つの整形済みファイルにまとめる前処理ツールです。
主に株式銘柄スクリーニングのデータ準備工程を自動化します。

## 入力ファイル
- 参照元：IR BANK
  [https://irbank.net/download](https://irbank.net/download)
- 対象データ：通期データ（2010～2025年）

## 出力ファイル
- `output/output.csv`
  前処理済みの統合CSVファイル

## 環境構築
### 前提
- Python 3.11 以上（推奨: 3.17）
- Windows 11 で動作確認済み

### 手順
```bash
# 仮想環境の作成
python -m venv .venv

# 仮想環境の有効化
.venv\Scripts\activate   # Windows
source .venv/bin/activate # Mac/Linux

# 依存ライブラリのインストール
pip install -r requirements.txt