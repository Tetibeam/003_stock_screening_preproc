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

def convert_columns_type(df: pd.DataFrame, columns: list[str], to_type: str, verbose: bool = True) -> pd.DataFrame:
    """
    指定列を文字列から任意の型に変換します。

    Args:
        df(pd.DataFrame) : 変換対象のデータフレーム
        columns(list[str]) :変換対象列名リスト
        to_type(str) : 変換先型。'int', 'float', 'str' のいずれか
        verbose(bool) : 変換ログを表示するか
    Returns: 
        pd.DataFrame: 変換後のデータフレーム（コピー）
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

        elif to_type == "float":
            # 無理な値は NaN にして float64 に
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

        elif to_type == "str":
            # NaN も含めて文字列化
            df[col] = df[col].astype(str)
        else:
            raise ValueError(f"変換先型 '{to_type}' はサポートされていません。")

    return df

def chk_finale_dtype(df: pd.DataFrame, expected_dtype: dict):
    """
    最終的なデータ型をチェックします。

    Args:
        df(pd.DataFrame): チェックするデータフレーム。
        expected_dtype(dict):
            キーが列名、値が期待されるデータ型（例: {"col1": "int64", "col2": "object"}）。
    Raises:
        TypeError: データ型が期待と異なる場合に発生します。

    """
    for col in df:
        actual_dtype_str = str(df[col].dtype)
        expected_dtype_str = expected_dtype[col]
        if actual_dtype_str != expected_dtype_str:
            # 🚨 致命的なエラーとして処理を中断
            raise TypeError(
                f"🚨 Dtypeチェック失敗: カラム '{col}' のデータ型が一致しません。"
                f"期待される型: '{expected_dtype_str}' | 実際の型: '{actual_dtype_str}'"
            )

def update_duplicated(df_by_files: pd.DataFrame, latest_year: int):
    """
    指定された最新年度のデータで、それ以前の年度の重複データを更新します。

    Args:
        df_by_files (pd.DataFrame): ファイルと年度をキーとするデータフレームの辞書。
        latest_year (int): 最新とみなす年度。

    Returns:
        dict: 更新されたデータフレームの辞書。

    """
    cutoff = pd.to_datetime(f"{latest_year}-01-01")
    return {
        (file, year): (
            (df_by_files[(file, year)]
             .set_index(["コード", "年度"])
             .update(
                df_by_files.get((file, latest_year), pd.DataFrame())
                .loc[lambda d: d["年度"] < cutoff]
                .set_index(["コード", "年度"])
             ) or df_by_files[(file, year)].set_index(["コード", "年度"])).reset_index()
        )
        for file in {f for f, _ in df_by_files.keys()}
        for year in {y for f, y in df_by_files.keys() if f == file}
        if (file, year) in df_by_files
    }