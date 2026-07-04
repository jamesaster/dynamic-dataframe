import pandas as pd

def load_and_normalize(csv_path_1: str, csv_path_2: str, config: dict) -> pd.DataFrame:
    """
    ## Stage 1 - Load & Normalize
    - Consolidate disparate datasets into a unified source, standardize header naming
    - `config`: to get drop_col list for column pruning.
    """
    # 1. Load
    df_1 = pd.read_csv(csv_path_1)
    df_2 = pd.read_csv(csv_path_2)

    # 2. Đồng bộ header vstack
    df_1.columns = df_2.columns 

    # 3. Vstack và (Dropna cho 4 cột đầu quan trọng)
    first_4_cols = df_1.columns[:4]
    df_raw = pd.concat([df_1, df_2], axis=0, ignore_index=True)
    df_raw = df_raw.dropna(subset=first_4_cols).reset_index(drop=True)

    # 4. Chuẩn hóa tên cột (Snake case)
    df_raw.columns = df_raw.columns.str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
    
    # 5. Loại bỏ cột thừa dựa trên config
    df = df_raw.drop(config.get('drop_col', []), axis=1, errors='ignore').copy()
    
    return df