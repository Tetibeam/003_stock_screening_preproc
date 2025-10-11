# 株式銘柄のスクリーニング前処理

## プロジェクト概要
今プロジェクトは株式銘柄スクリーニングのデータ準備工程です。CSVファイルを読み込み、欠損値・異常値を処理し、1つの整形済みファイルにまとめる前処理ツールです。

## 入力ファイル
- `data/西暦  
  財務データのCSVファイル群
- 参照元：IR BANK
  [https://irbank.net/download](https://irbank.net/download)
- 対象データ：通期データ（2010～2025年）

- data/  
  東証上場企業銘柄一覧
- 参照元：https://www.jpx.co.jp/markets/statistics-equities/misc/01.html

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