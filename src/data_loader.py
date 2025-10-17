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
