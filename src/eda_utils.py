# -*- coding: utf-8 -*-
import os
import pandas as pd

def chk_missing_values_expression(df: pd.DataFrame, filename: str, option_value: str) -> pd.DataFrame:
    """
    欠損値の表現をチェックします。
    Args:
        df（pd.DataFrame）: チェックするデータフレーム。
        filename(str): ファイル名。
        option_value(str): オプション値。
    Returns:
        pd.DataFrame: チェック結果のデータフレーム。
    """
    COUNT_COLUMNS = ["col","empty", "space", "-", "―", "—", "--", "Na", "na", "N/A", "n/a", "None", "none", "NULL", "null", "0", "0.0", "alphabet"]
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
            (ser_str == '0').sum(), (ser_str == '0.0').sum(),
            ser_str.str.contains('[A-Za-z]', na=False).sum()
        )
        df_placeholder_counts.loc[current_index, COUNT_COLUMNS] = row_data_tuple
        df_placeholder_counts.loc[current_index, "ファイル名"] = filename
        df_placeholder_counts.loc[current_index, "オプション値"] = option_value
        current_index += 1
    return df_placeholder_counts

def chk_missing_and_suspect(df_placeholder_counts: pd.DataFrame) -> dict:
    """
    重複している欠損値の表現をチェックします。
    Args:
        df_placeholder_counts: 欠損値の表現をカウントしたデータフレーム。
    Returns:
        dict: 重複している欠損値の表現と、それを含む列名の辞書。
    """
    df = df_placeholder_counts.drop(["ファイル名", "オプション値"], axis=1)
    # 値0以上の列だけ残す
    suspect_sum = df.groupby("col").sum().sum()[lambda x: x > 0]
    print(suspect_sum)
    # 結果を格納する辞書
    result_dict = {}
    for code in suspect_sum.index:
        cols_with_code = df.loc[df[code] > 0, "col"].unique()
        result_dict[code] = cols_with_code
    return result_dict

def chk_zero_data(df_merged_by_file: pd.DataFrame) ->pd.DataFrame:
    """
    指定されたデータフレーム内の各ファイルについて、"0"を持つデータがないかチェックします。

    Args:
        df_merged_by_file (pd.DataFrame): 複数のデータフレームを結合したデータフレーム。

    Returns:
        pd.DataFrame: "0"を持つデータが見つかった場合の、コード、対象列、ファイル名を含むデータフレーム。
                      見つからなかった場合は空のデータフレームを返します。

    """
    result_list = [
    df[df["コード"] == code].assign(
        対象列=col,
        ファイル名=fname
    )
    for fname, df in df_merged_by_file.items()
    for col in df.columns
    for code in df[df[col] == "0"]["コード"].unique()
]
    # 結果をまとめる
    if result_list:
        df_result = pd.concat(result_list, ignore_index=True)
    else:
        print("ゼロを含むコードは見つかりませんでした。")
        df_result = pd.DataFrame()
    return df_result

def chk_dtype(df: pd.DataFrame, filename:str, option_value:str, na_drop:bool=True) -> pd.DataFrame:
    """
    データ型をチェックします。
    Args:
        df(pd.DataFrame): チェックするデータフレーム。
        filename(str): ファイル名。
        option_value(str): オプション値。
        na_drop(bool): NaNを除外して型をチェックするかどうか。
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
