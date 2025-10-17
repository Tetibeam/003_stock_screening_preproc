# -*- coding: utf-8 -*-
import os
import pandas as pd


def chk_missing_values_expression(df, filename, option_value):
    """
    欠損値の表現をチェックします。
    Args:
        df (pd.DataFrame): チェックするデータフレーム。
        filename (str): ファイル名。
        option_value (str): オプション値。
    Returns:
        pd.DataFrame: チェック結果のデータフレーム。
    """
    COUNT_COLUMNS = ["col","empty", "space", "-", "―", "—", "--", "Na", "na", "N/A", "n/a", "None", "none", "NULL", "null", "0", "alphabet"]
    FINAL_COLUMNS = COUNT_COLUMNS + ["ファイル名", "オプション値"] # 追跡を明確にするため
    df_placeholder_counts = pd.DataFrame(columns=FINAL_COLUMNS)
    current_index = 0
    for col in df:
        ser = df[col].fillna(pd.NA)
        ser_str = ser.astype(str)
        #display(ser_str)
        row_data_tuple = (
            col,
            (ser_str == '').sum(), (ser_str == ' ').sum(),
            (ser_str == '-').sum(), (ser_str == '―').sum(), (ser_str == '—').sum(), (ser_str == '--').sum(),
            (ser_str == 'Na').sum(), (ser_str == 'na').sum(), (ser_str == 'N/A').sum(), (ser_str == 'n/a').sum(),
            (ser_str == 'None').sum(), (ser_str == 'none').sum(), (ser_str == 'NULL').sum(), (ser_str == 'null').sum(),
            (ser_str == '0').sum(), 
            ser_str.str.contains('[A-Za-z]', na=False).sum()
        )
        df_placeholder_counts.loc[current_index, COUNT_COLUMNS] = row_data_tuple
        df_placeholder_counts.loc[current_index, "ファイル名"] = filename
        df_placeholder_counts.loc[current_index, "オプション値"] = option_value
        current_index += 1
    return df_placeholder_counts

def chk_dtype(df: pd.DataFrame, filename:str, option_value:str, na_drop:bool=True) -> pd.DataFrame:
    """
    データ型をチェックします。
    Args:
        df (pd.DataFrame): チェックするデータフレーム。
        filename (str): ファイル名。
        option_value (str): オプション値。
    Returns:
        pd.DataFrame: チェック結果のデータフレーム。
    """
    COUNT_COLUMNS = ["ファイル名", "オプション値", "列名", "dtype", "sample_types"]
    df_type = pd.DataFrame(columns=COUNT_COLUMNS)
    current_index = 0
    for col in df.columns:
        # 実際のデータ型を確認
        dtype = df[col].dtype
        sample_types = df[col].dropna().map(type).unique() if na_drop else df[col].map(type).unique()
        row_data_tuple = (filename, option_value, col, dtype, sample_types)
        df_type.loc[current_index, COUNT_COLUMNS] = row_data_tuple
        #print(row_data_tuple)
        current_index +=1
    return df_type

def convert_columns_type(df: pd.DataFrame, columns: list[str], to_type: str, verbose: bool = True) -> pd.DataFrame:
    """
    指定列を文字列から任意の型に変換します。

    Args:
        df : 変換対象のデータフレーム
        columns :変換対象列名リスト
        to_type : 変換先型。'int', 'float', 'str' のいずれか
        verbose : 変換ログを表示するか
    Returns : 変換後のデータフレーム（コピー）
    """
    df = df.copy()

    for col in columns:
        if col not in df.columns:
            if verbose:
                print(f"[WARN] 列 '{col}' が存在しません。スキップします。")
            continue

        if verbose:
            print(f"\n列 '{col}' を {to_type} に変換中...")

        if to_type == "int":
            # 無理な値は NaN にして Int64(nullable) に
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            print("done")
        elif to_type == "float":
            # 無理な値は NaN にして float64 に
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif to_type == "str":
            # NaN も含めて文字列化
            df[col] = df[col].astype(str)
        else:
            raise ValueError(f"変換先型 '{to_type}' はサポートされていません。")

    return df





