from src.columns import colName as c, colRaw_mapping as colMap, colFormat as f
from src.stockledger import process_stockLedger
from concurrent.futures import ThreadPoolExecutor
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.discovery import build
from google.oauth2 import service_account
import google.auth.transport.requests
from pathlib import Path
from io import BytesIO
import streamlit as st
import pandas as pd
import duckdb

SS = st.session_state
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
    'DEMO_TRAFFIC.parquet',
    'random_forest.pkl',
    'ETP_stock_ledger.parquet'
]

@st.cache_resource(show_spinner='Khởi tạo kết nối tới Google API')
def cached_http_session():
    """
    ## Khởi tạo đường ống tới Google API (Thread-safe).

    Mục đích:
    ---------
    Thay vì để mỗi lần build service, worker phải tạo đường ống mới từ đầu và đập bỏ khi fetch xong.
    Hàm này sẽ tạo 1 đường ống ổn định đầu tiên và cached vào RAM.
    Hàm get_connections sẽ dùng build thêm nhánh trực tiếp từ ống chính, tiết kiệm thời gian và tài nguyên.

    Return:
    -------
    google.auth.transport.requests.Request: 
        Một HTTP session object đã được cấu hình sẵn để làm cổng vận chuyển dữ liệu cho Google API Client.
    """
    return google.auth.transport.requests.Request()
def get_google_connections(key = SECRET_KEY):
    """
    ## Khởi tạo và cấu hình các dịch vụ kết nối (Google Drive & Sheets) cho từng luồng.

    Mục đích:
    ---------
    Sử dụng đường ống chính đã Cached và build thêm các nhánh (v3, v4) để fetch data về máy.
    Sử dụng .refresh(cached_session) để verify token của cred còn hạn hay không.
    Nếu cred check với ống thấy token expired thì sẽ dùng ống để xin API cấp lại token (rất nhanh).
    Trả về:
    -------
    dict:
        - 'drive' : googleapiclient.discovery.Resource (Drive API v3 Client)
        - 'sheets': googleapiclient.discovery.Resource (Sheets API v4 Client)

    """
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    
    base_credentials = service_account.Credentials.from_service_account_info(key)
    credentials = base_credentials.with_scopes(scopes)
    
    cached_session = cached_http_session()
    credentials.refresh(cached_session)
    
    drive_service = build('drive', 'v3', credentials=credentials)
    sheets_service = build('sheets', 'v4', credentials=credentials)
    
    return {
        'drive': drive_service,
        'sheets': sheets_service
    }

@st.cache_data(ttl=43200, show_spinner='Fetching data from Google Sheets...')
def load_sales_sheet(
    sheet_id : str = '1o7DlHmsLAu8tdMtUplytq-5Tuh25o8OC0VgI_kUcFqA',
    gid      : int = 0,
    tab      : str = 'combine',
    columns  : str = '!A:AA'
) -> pd.DataFrame:
    """
    ## fetch data from googlesheets using gid_id, fallback with tab_name
    """
    try:
        connections = get_google_connections()
        service     = connections['sheets']
        try:
            sheets_metadata = service.spreadsheets().get(spreadsheetId = sheet_id).execute()
            tab = [m['properties']['title'] for m in sheets_metadata['sheets'] if m['properties']['sheetId']==gid][0]
        except Exception as e:
            print(f'gid did not match: {e}')
        
        range_name = f"'{tab}'{columns}"
        sheet_object = service.spreadsheets().values().get(
            spreadsheetId = sheet_id, 
            range = range_name
        ).execute()
        
        list_of_records = sheet_object.get('values', [])
        if not list_of_records:
            print('Sheet Empty')
            return pd.DataFrame()
        
        # Đằng nào cũng bị object toàn bộ, clean luôn cho sạch
        data = pd.DataFrame(data = list_of_records[1:], columns = colMap.values()).astype('string')
        mask = data.apply(lambda x: x.str.strip(), axis=0) == ''
        data = data.mask(mask)
        return data
    
    except Exception as e:
        print(f"Ối giồi ôi: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner='Fetching data from Google Drive...')
def load_files_from_drive(
    folder_id: str  = FOLDER_ID, 
    file_list: list = FILE_LIST
    ) -> dict:
    """
    ### Hàm đọc toàn bộ files yêu cầu từ Drive và Cached RAM.
    ### Chạy lần đầu ở app.py, các page khác khi gọi hàm sẽ không cần fetch lại.
    """
    
    def fetch_worker(file_name: str):
        try:
            # không thể bỏ connections ra hàm ngoài, mỗi worker cần có service riêng
            connections = get_google_connections()
            thread_service = connections['drive']
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
            elif ext == 'pkl':
                df = BytesIO(media_content)
            else:
                df = None
            return file_name, df
        except Exception:
            return file_name, None

    with ThreadPoolExecutor(max_workers=len(file_list)) as executor:
        results = executor.map(fetch_worker, file_list)
        
    return {file_name: df for file_name, df in results if df is not None}

@st.fragment
def upload_stockLedger(is_james: bool, google_service = get_google_connections()['drive']):
    if not is_james: return
    if not 'upload_worker' in SS:
        SS.upload_worker = 'pending'
        SS.upload_worker_counter = 0

    def upload_worker(parquet_buffer, google_service):
        buffer_to_media  = MediaIoBaseUpload(parquet_buffer, mimetype='application/octet-stream', resumable=False)
        try:
            updated_file = google_service.files().update(
                fileId     = '1EM0gi30at2Rb4cnxCr-C2vZ6CQl3PwpH',
                body       = {'name': 'ETP_stock_ledger.parquet'},     
                media_body = buffer_to_media,
                fields     = 'id, name'
            ).execute(num_retries = 3)
            SS.upload_worker = 'Done'
            print('[upload_worker] ⚡')
            
        except Exception as e:
            print(f"[upload_worker] Upload {updated_file.get('name')} Error: {e}")
            SS.upload_worker = False

    with st.popover(
        label    = '**Upload Stock Ledger**',
        icon     = ':material/upload_file:',
        width    = 'stretch',
        disabled = not is_james
        ):
        st.subheader('Dữ liệu ETP từ ngày mở cửa')

        if SS.upload_worker == 'dimiss':
            st.info('Upload Successful!')

        file = st.file_uploader(
            label            = 'Update Stock',
            type             = 'xls',
            key              =  f'update_stock_{SS.upload_worker_counter}',
            max_upload_size  =  50,
            label_visibility = 'collapsed'
        )

        if file is None or file.type != 'application/vnd.ms-excel':
            SS.upload_worker = 'pending'
            return
        
        raw_stock = pd.read_excel(BytesIO(file.getvalue()), engine='xlrd')
        if len(raw_stock.columns) != 19:
            return
        
        stock_ledger = process_stockLedger(raw_stock)
        if stock_ledger is None or stock_ledger.empty:
            return
        
        SS.stock_ledger = stock_ledger
        s_date = stock_ledger[c.date].iloc[0].strftime('%d-%m-%Y')
        e_date = stock_ledger[c.date].iloc[-1].strftime('%d-%m-%Y')
        
        st.info(
            f"""
            **File info:**
            - From:\u2000 {str(s_date)}
            - End:\u2000\u2000 {str(e_date)}
            """)

        parquet_buffer = BytesIO()
        stock_ledger.to_parquet(
            parquet_buffer,
            engine = 'pyarrow',
            compression = 'snappy',
            use_dictionary = True,
            index = False
        )
        parquet_buffer.seek(0)
        if st.button('Submit', width = 'stretch', icon = ':material/check:'):
            upload_worker(parquet_buffer, google_service)
            SS.upload_worker = 'dimiss'
            SS.upload_worker_counter += 1
            st.rerun(scope='fragment')

@st.cache_data
def get_local_data(
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
        raw.insert(1, 'month_year', raw[_date_col].dt.strftime(f.month))
        
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


def switch_to_auth(sales_data: pd.DataFrame):
    stock_ledger = (
        load_files_from_drive()
        ['ETP_stock_ledger.parquet']
        )
    
    stock_info = (
        sales_data
        .dropna(subset=c.price)
        .drop_duplicates(subset=c.sku, keep='last', ignore_index=True)
        [[c.sku, c.cat, c.subcat, c.price]]
        )
    
    if SS.get('stock_ledger') is not None:
        stock_ledger = SS.stock_ledger

    stock_ledger = stock_ledger.merge(stock_info, how='left', on=c.sku).dropna(how='any', ignore_index=True)
 
    min_date = sales_data[c.date].min()
    max_date = sales_data[c.date].max()

    return stock_ledger, sales_data, min_date, max_date


@st.cache_data
def get_demo_data(
    *,
    _SALES   :str = 'DASHBOARD_Run_Forest_Run.parquet',
    _LEDGER  :str = 'DASHBOARD_stock_ledger.parquet',
    _PRODUCT :str = 'DASHBOARD_master_product.csv'
) -> tuple[pd.DataFrame, pd.DataFrame, any, any]:
    
    file_list    = [_SALES, _LEDGER, _PRODUCT]
    all_raw_data = load_files_from_drive()
    
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
        raw.insert(1, c.month, raw[c.date].dt.strftime(f.month))
        
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
    max_date: pd.Timestamp 
) -> tuple[str, int]:
    """
    ### Xác định tháng hiện tại
    ### Xác định lọc 1 tháng gần nhất là lấy bao nhiêu ngày
    """
    now_date = max_date
    prev_month = now_date - pd.DateOffset(months=1)
    curr_month = now_date.strftime('%B')
    prev_month_days = (now_date - prev_month).days

    return curr_month, prev_month_days


if __name__ == '__main__':
 pass