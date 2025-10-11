# -*- coding: utf-8 -*-
import os
import pandas as pd

def merge_all_data(all_df_by_files):
    """
    すべてのデータを結合します。

    Args:
        all_df_by_files (dict): ファイル名をキーとし、DataFrameを値とする辞書。

    Returns:
        pd.DataFrame: 結合されたDataFrame。
    """
    merged = all_df_by_files[next(iter(all_df_by_files))].copy()
    for filename in list(all_df_by_files.keys())[1:]:
        df = all_df_by_files[filename]
        merged = pd.merge(merged, df, on=["コード", "年度"], how="outer")
    return merged

def filter_code_by_latest_year(df):
    """
    最新の年に上場している銘柄のコードリストを返します。

    Args:
        df (pd.DataFrame): 列に"コード"、"年度"を含むDataFrame。

    Returns:
        pd.DataFrame: 最新の年に上場している銘柄のコードリスト
    """
    df["西暦"] = df["年度"].astype(str).str[:4].astype(int)
    #latest_year = df["西暦"].max()
    latest_year = 2025
    print(latest_year)
    latest_df = df[df["西暦"] == latest_year]["コード"].to_list()
    return latest_df