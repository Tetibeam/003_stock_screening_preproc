# -*- coding: utf-8 -*-
import os
import pandas as pd
import dateutil
def load_data_by_files(base_path, years, files, na_values=[""]):
    """
    ファイルごとに読み込み、辞書として返します。

    Args
        base_path (str): データが格納されているベースパス。
        years (list): 読み込む年度のリスト。
        files (list): 読み込むファイル名のリスト。
        na_values (list): 欠損値として扱う値のリスト。
    Returns:
        dict: ファイル名+年度をキーとし、DataFrameを値とする辞書。

    """
    df_by_files = {}
    for filename in files:
        for year in years:
            file_path = os.path.join(base_path, str(year), filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, header=1, na_values=na_values, dtype={'コード': str, '年度': str})
                df["年度"] = pd.to_datetime(df["年度"], format="%Y/%m")
                df_by_files[(filename, year)] = df
            else:
                df_by_files[(filename, year)] = pd.DataFrame()
    return df_by_files

def update_duplicated(df_by_files,latest_year):
    """
    重複しているデータを更新し,削除します
    Args:
        df_by_files (dict): ファイル名+年度をダブルキーとし、DataFrameを値とする辞書。
        year (str): 更新する年度
    Returns:
        dict: ファイル名+年度をキーとし、DataFrameを値とする辞書。

    """
    df_all_datas_by_files = {}
    cutoff_date = cutoff_date = pd.to_datetime(f"{str(latest_year)}-01-01")
    #print(cutoff_date)
    all_files = {key[0] for key in df_by_files.keys()}
    for file in all_files:
        df_source_raw = df_by_files.get((file, latest_year))
        if df_source_raw is None:
            print(f"警告: {file} の2025年データが見つかりません。スキップします。")
            continue
        # フィルタリング条件を作成: '年度'が基準日より小さいデータのみ抽出
        df_update_source = df_source_raw[df_source_raw["年度"] < cutoff_date]
        df_update_source = df_update_source.set_index(["コード", "年度"])
        #display(df_update_source)
        # --- 2. 過去の全年のデータを更新 ---
        all_years = {key[1] for key in df_by_files.keys() if key[0] == file}
        for year in all_years:
            df_target = df_by_files.get((file, year))
            if df_target is None:
                continue
            df_target_indexed = df_target.set_index(["コード", "年度"])

            df_target_indexed.update(df_update_source)
            df_target_indexed = df_target_indexed.reset_index()
            df_all_datas_by_files[(file, year)] = df_target_indexed
    return df_all_datas_by_files

def load_yearly_data(base_path, years, files, na_values=[""]):
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

def chk_yearly_header(base_path, years, files):
    """
    年次データをファイルごとに読み込み、列名を表示します。

    Args:
        base_path (str): データが格納されているベースパス。
        years (list): 読み込む年度のリスト。
        files (list): 読み込むファイル名のリスト。

    """
    col_list = []
    for filename in files:
        for year in years:
            file_path = os.path.join(base_path, str(year), filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path,header=1)
                col = df.columns.tolist()
                col_list.append((filename, year, col))
    return col_list