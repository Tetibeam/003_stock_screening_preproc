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
def get_latest_year_code_list(df, year):
    """
    指定された年度のコードリストを取得します。

    Args:
        df (pd.DataFrame): データフレーム。
        year (int): 取得する年度。

    Returns:
        list: 指定された年度のコードリスト。
    """
    return df[df['年度'] == year]['コード'].unique().tolist()
def filter_by_latest_year_code(df):
def get_code_info_by_latest_year():
    """
    最新年度のコード情報を取得します。

    Returns:
        pd.DataFrame: 最新年度のコードリストを含むDataFrame。
    """
    return pd.read_csv(os.path.join("data", "latest_year_code_list.csv"), dtype={'コード': str})