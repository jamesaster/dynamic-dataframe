from pathlib import Path
import pandas as pd
import numpy as np  
import duckdb

def join_sales_traffic(df: any, traffic_path: any, demo: bool = True) -> pd.DataFrame:
    """
    ### Join sales với traffic bằng DuckDB
    - df: có thể là đường dẫn (str/Path) hoặc DataFrame
    - traffic_path: có thể là đường dẫn (str/Path) hoặc DataFrame
    """

    if isinstance(df, pd.DataFrame):
        df_input = "df"
    elif isinstance(df, (str, Path)):
        df_input = f"'{df}'"
    else:
        raise TypeError("df must be either a string path or a pandas DataFrame")

    if isinstance(traffic_path, pd.DataFrame):
        traffic_input = "traffic_path"
    elif isinstance(traffic_path, (str, Path)):
        traffic_input = f"'{traffic_path}'"
    else:
        raise TypeError("traffic_path must be either a string path or a pandas DataFrame")

    _traffic = 'new_Traffic' if demo else 'Traffic'
    query = f"""
        SELECT 
            * EXCLUDE (Period, {_traffic}, Event_name),
            {_traffic} as date_traffic,
            Event_name as event_name 
        FROM {df_input} s
        LEFT JOIN {traffic_input} t
            ON s.date = t.Period
        ORDER BY date ASC;
    """
    
    return duckdb.query(query).df().convert_dtypes()

def bf_fill(df: pd.DataFrame, _anchor: str, _target_cols: list)-> pd.DataFrame:
    """
    ### Backward-Forward Fill cho các cột target dựa trên anchor
    - _anchor: cột dùng để xác định nhóm (ví dụ: invoice)
    """
    df[_target_cols] = df[_target_cols].replace(r'^\s*$', np.nan, regex=True)
    df[_target_cols] = df.groupby(_anchor)[_target_cols].transform('bfill')
    df[_target_cols] = df.groupby(_anchor)[_target_cols].transform('ffill')
    return df