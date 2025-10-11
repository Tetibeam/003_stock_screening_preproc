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
    for filename in files:
        for year in years:
            file_path = os.path.join(base_path, str(year), filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path,header=1)
                print("年：",year," ","ファイル名:",filename," ","列",df.columns.to_list())
