# -*- coding: utf-8 -*-
import os
import pandas as pd

def load_yearly_data(base_path, years, files, na_values=["-", "0"]):
    """
    年次データをファイルごとに読み込み、辞書として返します。

    Args:
        base_path (str): データが格納されているベースパス。
        years (list): 読み込む年度のリスト。
        files (list): 読み込むファイル名のリスト。
        na_values (list, optional): 欠損値として扱う値のリスト。Defaults to ["-", "0"].

    Returns:
        dict: ファイル名をキーとし、DataFrameを値とする辞書。
    """
    all_df_by_files = {}
    for filename in files:
        yearly_dfs = []
        for year in years:
            file_path = os.path.join(base_path, str(year), filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, header=1, na_values=na_values, dtype={'コード': str})
                yearly_dfs.append(df)
        if yearly_dfs:
            all_df_by_files[filename] = pd.concat(yearly_dfs, ignore_index=True)
        else:
            all_df_by_files[filename] = pd.DataFrame()

    return all_df_by_files

def load_code_list_info(base_path, filename):
    """
    コードリスト情報のファイルを読み込み、データフレームを返します。

    Args:
        base_path (str): データが格納されているベースパス。
        files (str): 読み込むファイル名。

    Returns:
        dict: ファイル名をキーとし、DataFrameを値とする辞書。
    """
    file_path = os.path.join(base_path, filename)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, dtype={'コード': str})
        code_list_info = df
    else:
        code_list_info[filename] = pd.DataFrame()
    return code_list_info