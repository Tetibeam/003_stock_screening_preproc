# -*- coding: utf-8 -*-
import os
import pandas as pd

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

def chk_missing_value_exploration(df):
    """
    Nanの表現方法を探索します。

    Args:
        df (pd.DataFrame): 探索するDataFrame。
    Return:
        pd.DataFrame：各表現を列にとった数
    """
    #"nan", "na", "n/a", "null", "-", "--", "none", ""
    counts = []
    for col in df:
        ser = df[col].fillna('')
        counts.append(
            (col,
            (ser == 'nan').sum(),
            (ser == 'na').sum(),
            (ser == 'n/a').sum(),
            (ser == '-').sum(),
            (ser == '--').sum(),
            (ser == 'none').sum(),
            (ser == '0').sum(),
            (ser == '').sum(),
            ser.str.contains('[A-Za-z]', na=False).sum())
        )
    placeholder_counts = pd.DataFrame(
        counts, 
        columns=["col","nan","na","n/a","-","--","none","0","","alphabet"]
    )
    return placeholder_counts

def chk_listed_company(df, code_list):
    """
    コードリストとデータにあるコードを比較します

    Args:
        df (pd.DataFrame): 列に"コード"を含むDataFrame。
        code_list (list): 銘柄のコードリスト


    """
    

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