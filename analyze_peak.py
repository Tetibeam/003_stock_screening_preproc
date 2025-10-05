
import yaml
import os
import pandas as pd

# 設定読み込み
try:
    with open("setting.yaml", "r", encoding="utf-8") as f:
        setting = yaml.safe_load(f)
except FileNotFoundError:
    print("Error: setting.yamlが見つかりません。")
    exit()

base_path = setting.get("data_path", "data")
years = setting.get("years", [])
files = setting.get("files", [])

if not years or not files:
    print("Error: setting.yamlにyearsまたはfilesが正しく設定されていません。")
    exit()

# 全ファイル結合
all_df = {}
for filename in files:
    all_df[filename] = pd.DataFrame()
    for year in years:
        file_path = os.path.join(base_path, str(year), filename)
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, header=1, na_values="-")
                all_df[filename] = pd.concat([all_df[filename], df], ignore_index=True)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")


# 企業ごとに欠損率を計算
missing_score = {}
for filename in files:
    if "コード" in all_df[filename].columns:
        # Ensure numeric conversion for aggregation
        numeric_cols = all_df[filename].select_dtypes(include=pd.np.number).columns.tolist()
        # Create a lambda that calculates isnull().mean() only for existing columns in the group
        agg_func = lambda x: x.isnull().mean()
        
        # Group by 'コード' and aggregate
        grouped = all_df[filename].groupby("コード")
        
        # Calculate missing mean per group
        missing_mean = grouped.apply(agg_func).mean(axis=1)

        missing_score[filename] = missing_mean.reset_index(name="平均欠損率")
    else:
        missing_score[filename] = pd.DataFrame(columns=["コード", "平均欠損率"])


# 該当範囲の企業を抽出 (fy-balance-sheet.csv の結果を使用)
target_filename = "fy-balance-sheet.csv"
if not missing_score.get(target_filename, pd.DataFrame()).empty:
    peak_group = missing_score[target_filename][
        (missing_score[target_filename]["平均欠損率"] >= 0.215) &
        (missing_score[target_filename]["平均欠損率"] <= 0.225)
    ]

    print(f"該当する企業数: {len(peak_group)}社")
    print("--- 該当企業（一部） ---")
    print(peak_group.head().to_string())
else:
    print(f"{target_filename}の欠損率を計算できませんでした。")
