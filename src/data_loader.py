# -*- coding: utf-8 -*-
import os
import pandas as pd
import hashlib
from collections import defaultdict

def load_on_startup(base_path, folder, file, header):
    """
    ファイルをデータフレームで読み込みます。（型は強制的にstr）
    
    Args:
        base_path(str):ファイルが格納されているベースパス。
        folder(str):フォルダ名
        file(str):ファイル名
    Return:
        pd.DataFrame: 読み込んだファイル
    """
    file_path = os.path.join(base_path, folder, file)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, header=header, keep_default_na=False, na_filter=False, dtype=str)
    else:
        df = pd.DataFrame()
    return df

def chk_file_missing(df):
    """
    データフレームが空か可動化を調べます。
    Args:
        df (pd.DataFrame): チェックするデータフレーム。
    Returns:
        flg(bool):
            True:ファイルが空でない
            False:ファイルが空
    """
    flg = True
    if df.empty:
        print("❌ ","データフレームが空です")
        flg = False
    return flg

def df_hash(df, ignore_order=False):
    """

    """
    data_only = df.copy().fillna("").astype(str)
    if ignore_order:
        # 行順も無視（全列ソートしてから比較）
        data_only = data_only.sort_values(by=data_only.columns.tolist()).reset_index(drop=True)
    # 列名を無視して内容だけでハッシュを生成
    h = hashlib.md5(pd.util.hash_pandas_object(data_only, index=True).values)
    return h.hexdigest()

def chk_duplicate_dfs(df_dict, ignore_order=False, ignore_column_order=False):
    """
    複数の DataFrame の中から重複しているものを検出する。
    
    Args:
        df_dict(dict) :
            キーが任意の識別子（例: (filename, year)）、値が DataFrame の辞書。
        ignore_order(bool, default False) :
            行順を無視して比較するかどうか。
        ignore_column_order(bool, default False) :
            列順を無視して比較するかどうか。
    Returns:
        duplicates(dict) :
            重複しているハッシュをキーに、該当する識別子リストを値とする辞書。
    """
    hash_map = defaultdict(list)
    for key, df in df_dict.items():
        data_only = df.copy().fillna("").astype(str)
        if ignore_order:
            # 行順を無視
            data_only = data_only.sort_values(by=data_only.columns.tolist()).reset_index(drop=True)
        if ignore_column_order:
            # 列順を無視
            data_only = data_only.reindex(sorted(data_only.columns), axis=1)
        # PandasオブジェクトのハッシュをまとめてMD5に
        h = hashlib.md5(pd.util.hash_pandas_object(data_only, index=True).values).hexdigest()
        hash_map[h].append(key)
    # 同じ内容が複数あるものだけ抽出
    duplicates = {h: keys for h, keys in hash_map.items() if len(keys) > 1}
    return duplicates









def load_data_by_files(base_path, years, files):
    """
    ファイルごとに読み込み、辞書として返します。

    Args
        base_path (str): データが格納されているベースパス。
        years (list): 読み込む年度のリスト。
        files (list): 読み込むファイル名のリスト。
    Returns:
        dict: ファイル名+年度をキーとし、DataFrameを値とする辞書。

    """
    df_by_files = {}
    for filename in files:
        for year in years:
            file_path = os.path.join(base_path, str(year), filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, header=1, keep_default_na=False, na_filter=False, dtype=str)
                #df["年度"] = pd.to_datetime(df["年度"], format="%Y/%m")
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