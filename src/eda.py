# -*- coding: utf-8 -*-
import os
import pandas as pd
from IPython.display import display

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

def chk_listed_company(df, code_list):
    """
    コードリストとデータに乗っているコードを比較します

    Args:
        df (pd.DataFrame): 列に"コード"を含むDataFrame。
        code_list (list): 銘柄のコードリスト


    """

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
