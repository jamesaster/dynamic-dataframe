from io import BytesIO
from pathlib import Path
import streamlit as st
import pandas as pd
import duckdb
from concurrent.futures import ThreadPoolExecutor
from src.columns import colName as c, stockCol as s, colRaw_mapping as colMap
from googleapiclient.discovery import build
from google.oauth2 import service_account

SECRET_KEY      = st.secrets['gcs_connections']
FOLDER_ID       = '1ti2XBRVZeXtuBEqDlp8pKQjE-moUe253'
FILE_LIST       = [
    'DASHBOARD_Run_Forest_Run.parquet',
    'DASHBOARD_stock_ledger.parquet',
    'DASHBOARD_master_product.csv',
    'DEMO_stock_ledger_dummy.csv',
    'DEMO_price_cat_ledger.csv',
    'DEMO_Anonym_Price.csv',
    'DEMO_sales_dummy_1000.csv',
    'DEMO_TRAFFIC.parquet'
]


@st.cache_data(ttl=43200, show_spinner='Fetching data from Google Sheets...')
def load_sales_sheet(
    sheet_id  : str = '1o7DlHmsLAu8tdMtUplytq-5Tuh25o8OC0VgI_kUcFqA',
    range_name: str = 'combine!A:X'
) -> pd.DataFrame:
    try:
        secret_key  = SECRET_KEY
        credentials = service_account.Credentials.from_service_account_info(secret_key)
        service     = build('sheets', 'v4', credentials=credentials)
        
        sheet_object = service.spreadsheets().values().get(
            spreadsheetId = sheet_id, 
            range = range_name
        ).execute()
        
        list_of_records = sheet_object.get('values', [])
        
        if not list_of_records:
            return pd.DataFrame()
            
        return pd.DataFrame(data = list_of_records[1:], columns = colMap.values()).convert_dtypes()
        
    except:
        return pd.DataFrame()
    
@st.cache_data(show_spinner='Fetching data from Google Drive, this may take a few seconds...')
def load_all_from_drive(
    folder_id: str  = FOLDER_ID, 
    file_list: list = FILE_LIST
    ) -> dict:
    """
    ### Hàm đọc toàn bộ files yêu cầu từ Drive và Cached RAM.
    ### Chạy lần đầu ở app.py, các page khác khi gọi hàm sẽ không cần fetch lại.
    """
    secret_key  = SECRET_KEY
    credentials = service_account.Credentials.from_service_account_info(secret_key)

    def fetch_worker(file_name: str):
        try:
            thread_service = build('drive', 'v3', credentials=credentials)
            query = f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
            file_infos = thread_service.files().list(q=query, fields="files(id)").execute()
            files = file_infos.get('files', [])
            
            if not files:
                return file_name, None
                
            file_id = files[0]['id']
            media_content = thread_service.files().get_media(fileId=file_id).execute()
            ext = file_name.split('.')[-1].lower()
            
            if ext in ['xlsx', 'xls']:
                df = pd.read_excel(BytesIO(media_content))
            elif ext == 'csv':
                df = pd.read_csv(BytesIO(media_content))
            elif ext == 'parquet':
                df = pd.read_parquet(BytesIO(media_content))
            else:
                df = None
            return file_name, df
        except Exception:
            return file_name, None

    with ThreadPoolExecutor(max_workers=len(file_list)) as executor:
        results = executor.map(fetch_worker, file_list)
        
    return {file_name: df for file_name, df in results if df is not None}

@st.cache_data
def get_streamlit_data(
    *,
    path_raw: Path, 
    path_saved_ledger: Path, 
    path_product_master: Path, 
    _date_col: str = c.date
) -> tuple[pd.DataFrame, pd.DataFrame, any, any]:
    """
    ## Read from hard drive path.
    - Hàm gộp tối ưu: Đọc dữ liệu, xử lý SQL và tạo sẵn các cột time metrics.
    """
    # --- 1. XỬ LÝ RAW DATA & TIME METRICS ---
    raw = duckdb.read_parquet(str(path_raw)).to_arrow_table().to_pandas()
    
    min_date, max_date = None, None
    if _date_col in raw.columns:
        raw[_date_col] = pd.to_datetime(raw[_date_col])
        min_date = raw[_date_col].min()
        max_date = raw[_date_col].max()
        
        # Tạo cột month_year
        raw.insert(1, 'month_year', raw[_date_col].dt.strftime('%Y-%m'))
        
        # Tạo cột week: format '25-W36 (01 Sep)'
        monday = raw[_date_col] - pd.to_timedelta(raw[_date_col].dt.weekday, unit='D')
        raw.insert(1, 'week', monday.dt.strftime('%g-W%V') + monday.dt.strftime(' (%d %b)'))
    else:
        st.warning(f'{_date_col} Does not exist in raw source!')

    # Downcast các cột số
    for col in raw.select_dtypes(include='number').columns:
        raw[col] = pd.to_numeric(raw[col], errors='coerce', downcast='integer')

    # --- 2. XỬ LÝ LEDGER & PRODUCT MASTER (Giữ nguyên SQL tối ưu) ---
    query = f"""
        WITH unique_product AS (
            SELECT DISTINCT 
                master_sku AS sku, 
                CASE 
                    WHEN cat = 'ACCESSORIES (APPLE)' THEN 'APPLE ACC' 
                    ELSE cat 
                END AS cat,
                LOWER(detail_sub_lob) AS detail_sub_lob, 
                new_price AS price
            FROM read_csv_auto('{str(path_product_master)}')
        ),
        ledger_clean AS (
            SELECT * EXCLUDE (sku, master_sku), master_sku AS sku
            FROM read_parquet('{str(path_saved_ledger)}')
            WHERE master_sku IS NOT NULL
        )
        SELECT l.*, p.cat, p.detail_sub_lob, p.price
        FROM ledger_clean AS l
        LEFT JOIN unique_product AS p ON l.sku = p.sku;
    """
    stock_ledger = duckdb.query(query).to_arrow_table().to_pandas()

    # --- 3. ĐỒNG BỘ PRODUCT_NAME ---
    product_name_map = (
        stock_ledger[['sku', 'product_name']]
        .drop_duplicates(subset=['sku'])
        .set_index('sku')['product_name']
    )
    raw['product_name'] = raw['sku'].map(product_name_map)

    return stock_ledger, raw, min_date, max_date

@st.cache_data
def get_streamlit_data_from_drive(
    *,
    _SALES  : str,
    _LEDGER : str,
    _PRODUCT: str
) -> tuple[pd.DataFrame, pd.DataFrame, any, any]:
    
    print('get_streamlit_data_from_drive - optimized')

    file_list    = [_SALES, _LEDGER, _PRODUCT]
    all_raw_data = load_all_from_drive()
    
    for name in file_list:
        if name not in all_raw_data:
            st.error(f'Gặp lỗi khi load file {name}')
            return pd.DataFrame(), pd.DataFrame(), None, None

    # Bốc thẳng dữ liệu từ RAM (2 df dưới chỉ cần tạo biến để duckdb đọc ngầm)
    df_clean_sales    = all_raw_data[_SALES]
    df_clean_ledger   = all_raw_data[_LEDGER]
    df_product_master = all_raw_data[_PRODUCT]

    raw = df_clean_sales
    
    # Require Time metrics
    min_date, max_date = None, None
    if c.date in raw.columns:
        raw[c.date] = pd.to_datetime(raw[c.date])
        min_date = raw[c.date].min()
        max_date = raw[c.date].max()
        
        # Tạo cột month_year
        raw.insert(1, c.month, raw[c.date].dt.strftime('%Y-%m'))
        
        # Tạo cột week: format '25-W36 (01 Sep)'
        monday = raw[c.date] - pd.to_timedelta(raw[c.date].dt.weekday, unit='D')
        raw.insert(1, c.week, monday.dt.strftime('%g-W%V') + monday.dt.strftime(' (%d %b)'))
    else:
        st.warning(f'{c.date} Does not exist in sales data!')

    # Downcast các cột số
    raw_num_cols = raw.select_dtypes(include='number').columns
    raw[raw_num_cols] = raw[raw_num_cols].apply(pd.to_numeric, errors='coerce', downcast='integer')

    # Query Duckdb
    query_sql = """
        WITH unique_product AS (
            SELECT DISTINCT 
                master_sku AS sku, 
                CASE 
                    WHEN cat = 'ACCESSORIES (APPLE)' THEN 'APPLE ACC' 
                    ELSE cat 
                END AS cat,
                LOWER(detail_sub_lob) AS detail_sub_lob, 
                new_price AS price
            FROM df_product_master
        ),
        ledger_clean AS (
            SELECT * EXCLUDE (sku, master_sku), master_sku AS sku
            FROM df_clean_ledger
            WHERE master_sku IS NOT NULL
        )
        SELECT l.*, p.cat, p.detail_sub_lob, p.price
        FROM ledger_clean AS l
        LEFT JOIN unique_product AS p ON l.sku = p.sku;
    """
    stock_ledger = duckdb.query(query_sql).to_arrow_table().to_pandas()

    # Map product_name
    product_name_map = (
        stock_ledger[[c.sku, c.prod_name]]
        .drop_duplicates(subset=[c.sku])
        .set_index(c.sku)[c.prod_name]
    )
    raw[c.prod_name] = raw[c.sku].map(product_name_map)

    return stock_ledger, raw, min_date, max_date


def get_current_past_config(
    df: pd.DataFrame, 
    _date: str = c.date
    ) -> tuple[str, int]:
    """
    ### Xác định tháng hiện tại
    ### Xác định lọc 1 tháng gần nhất là lấy bao nhiêu ngày
    """
    # (Today or Max date)
    now_date = pd.Timestamp.today().normalize()
    if not (df[_date] == now_date).any():
        now_date = df[_date].max()

    prev_month = now_date - pd.DateOffset(months=1)
    curr_month = now_date.strftime('%B')
    prev_month_days = (now_date - prev_month).days

    return curr_month, prev_month_days