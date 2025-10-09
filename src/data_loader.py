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
    all_df = {}
    for filename in files:
        yearly_dfs = []
        for year in years:
            file_path = os.path.join(base_path, str(year), filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, header=1, na_values=na_values, dtype={'コード': str})
                yearly_dfs.append(df)
        if yearly_dfs:
            all_df[filename] = pd.concat(yearly_dfs, ignore_index=True)
        else:
            all_df[filename] = pd.DataFrame()

    return all_df
def load_yearly_header(base_path, years, files):
    """
    年次データをファイルごとに読み込み、列名を表示します。

    Args:
        base_path (str): データが格納されているベースパス。
        years (list): 読み込む年度のリスト。
        files (list): 読み込むファイル名のリスト。

    """
    for filename in files:
        for year in years:
            file_path = os.path.join(base_path, str(year), filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path,header=1)
                print("年：",year," ","ファイル名:",filename," ","列",df.columns.to_list())
