# -*- coding: utf-8 -*-
import os
import pandas as pd
import hashlib
from collections import defaultdict

def load_on_startup(
        base_path: str, folder: str, file: str,
        header: int = 0, na_values: list[str] = [], force_str: bool=False
    ):
    """
    指定されたパスからCSVファイルを読み込み、データフレームを返します。

    Args:
        base_path(str): ベースパス。
        folder(str): フォルダ名。
        file(str): ファイル名。
        header(int): ヘッダー行。デフォルトは0。
        na_values(list[str]): 欠損値として解釈する文字列のリスト。
        force_str(bool): 文字列として読み込むかどうか。デフォルトはFalse。

    Returns:
        pd.DataFrame: 読み込まれたデータフレーム。ファイルが存在しない場合は空のデータフレーム。

    """
    file_path = os.path.join(base_path, folder, file)
    if os.path.exists(file_path):
        if force_str:
            df = pd.read_csv(file_path, header=header, dtype=str)
        else:
            df = pd.read_csv(file_path, header=header, na_values=na_values)
    else:
        raise FileNotFoundError(f"ファイル '{file_path}' が見つかりません。")
    return df

def chk_file_missing(df: pd.DataFrame):
    """
    データフレームが空か可動化を調べます。
    Args:
        df(pd.DataFrame): チェックするデータフレーム。
    Returns:
        fg(bool):
            True:ファイルが空でない
            False:ファイルが空
    """
    flg = True
    if df.empty:
        raise KeyError("データフレームが空です")
        flg = False
    return flg

def df_hash(df: pd.DataFrame, ignore_order: bool = False):
    """
    データフレームのハッシュ値を計算します。

    Args:
        df (pd.DataFrame): ハッシュ値を計算するデータフレーム。
        ignore_order (bool): Trueの場合、行の順序を無視します。デフォルトはFalse。

    Returns:
        str: データフレームのMD5ハッシュ値。
    """
    data_only = df.copy().fillna("").astype(str)
    if ignore_order:
        # 行順も無視（全列ソートしてから比較）
        data_only = data_only.sort_values(by=data_only.columns.tolist()).reset_index(drop=True)
    # 列名を無視して内容だけでハッシュを生成
    h = hashlib.md5(pd.util.hash_pandas_object(data_only, index=True).values)
    return h.hexdigest()

def chk_duplicate_files_df(duplicates:dict):
    """
    データフレームの重複をチェックし、重複がある場合はエラーを発生させます。

    Args:
        duplicates (dict): 重複するデータフレームのハッシュとキーの辞書。
                           `chk_duplicate_dfs` 関数によって生成されます。

    Raises:
        ValueError: 重複するデータフレームが検出された場合に発生します。

    """
    if duplicates:
        msg_lines = []
        for h, key_group in duplicates.items():
            msg_lines.append(f"ハッシュ {h} の重複:")
            for key in key_group:
                msg_lines.append(f"  - {key}")

        msg = "\n".join(msg_lines)
        raise ValueError(f"重複データが検出されました:\n{msg}")

def chk_duplicate_dfs(df_dict: dict, ignore_order: bool =False, ignore_column_order: bool=False):
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
