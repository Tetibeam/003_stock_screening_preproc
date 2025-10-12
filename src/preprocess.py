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

def filter_by_code_list(df, code_list):
    """
    コードリストのみのデータににフィルタリングします

    Args:
        df (pd.DataFrame): 列に"コード"を含むDataFrame。
        code_list (list): 銘柄のコードリスト

    Returns:
        pd.DataFrame: 上場している銘柄のコードリストでフィルタしたデータ
    """
    filetered_df = df[df["コード"].isin(code_list)]
    return filetered_df