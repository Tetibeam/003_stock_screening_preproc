# -*- coding: utf-8 -*-
import os
import pandas as pd

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

def chk_duplicated_data(df_by_files: dict)->dict:
    """
    Args:
        df_by_files (pd.DataFrame): 結合されたデータフレーム。

    Returns:
        pd.DataFrame: 重複データのデータフレーム。
    """

    results = {"lower": [], "upper": []}

    for (filename, year), df in df_by_files.items():
        conditions = {
            "lower": df["年度"].dt.year < year,
            "upper": df["年度"].dt.year > year
        }
        for key, cond in conditions.items():
            df_tmp = df.loc[cond, ["コード", "年度"]].copy()
            if not df_tmp.empty:
                df_tmp["ファイル名"] = filename
                df_tmp["フォルダ名"] = year
                results[key].append(df_tmp)

    df_result_lower = pd.concat(results["lower"], ignore_index=True) if results["lower"] else pd.DataFrame()
    df_result_upper = pd.concat(results["upper"], ignore_index=True) if results["upper"] else pd.DataFrame()
    return df_result_lower, df_result_upper

def update_duplicated_data(df_by_files: dict, df_result_lower: pd.DataFrame):
    """
    指定された最新年度のデータで、それ以前の年度の重複データを更新します。

    Args:
        df_by_files (pd.DataFrame): ファイルと年度をキーとするデータフレームの辞書。
        df_result_lower (pd.DataFrame): 下限を超える重複データのデータフレーム。

    Returns:
        dict: 更新されたデータフレームの辞書。
    """
    df_tmp = df_by_files.copy()
    for (f, y), df in df_by_files.items():
        df_update_date = df_result_lower.query("ファイル名 == @f and フォルダ名 == @y")
        if df_update_date.empty:
            continue
        df_update_source = df.set_index(["コード", "年度"])
        df_update_source = df_update_source.loc[
            df_update_date.set_index(["コード", "年度"]).index
        ]
        for key, df_target in df_tmp.items():
            df_target_indexed = df_target.set_index(["コード", "年度"])
            df_target_indexed.update(df_update_source)
            df_tmp[key] = df_target_indexed.reset_index()
    df_by_files_updated = df_tmp.copy()
    return df_by_files_updated


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