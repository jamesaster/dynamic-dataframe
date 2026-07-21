import streamlit as st
import io
import os
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from contextlib import redirect_stdout
from src import *
from core import *


BASE_DIR = Path(__file__).parent.parent
# SECRET_ENV_PATH         = BASE_DIR / 'config' / 'SECRET.env'
# DYNAMIC_PRICE_PATH      = BASE_DIR / 'config' / 'DYNAMIC_PRICE.json'
# SALES_2024_CLEAN_PATH   = BASE_DIR / 'CSV_read_only' / 'SALES_APPLE_2024_CLEAN.csv'
# SALES_2025_DIRTY_PATH   = BASE_DIR / 'CSV_read_only' / 'SALES_APPLE_2025_DIRTY.csv'
# TRAFFIC_DATA_PATH       = BASE_DIR / 'CSV_read_only' / 'APPLE_2024_2026_FAKE_TRAFFIC.parquet'
# ANONYM_PRICE_PATH       = BASE_DIR / 'data_output' / 'Anonym_Price.csv'
# DUMMY_PATH              = BASE_DIR / 'CSV_read_only' / 'sales_dummy_1000.csv'

@st.cache_data
def get_config():
    # load_dotenv(dotenv_path=SECRET_ENV_PATH)
    # with open(DYNAMIC_PRICE_PATH, 'r', encoding='utf-8') as js:
    #     scale_dict = json.load(js)
    
    config = {
        'payment_cols': ['cash', 'card', 'payoo', 'banking', 'mkt', 'vnpay', 'trade_in'],
        'disc_cols': ['disc_percent', 'disc_amount'],
        'date_anchor': 'invoice', 'date_pocket': 'date',
        'drop_col': ['vat', 'note'],
        'anonymous': ['phone', 'name', 'email']
    }
    return config

@st.fragment
def sales_pipeline_demo(raw_df: pd.DataFrame, anonym: pd.DataFrame, traffic: pd.DataFrame):
    # Tiêu đề Header
    st.caption('Hệ thống tự động chuẩn hóa, làm sạch và hợp nhất dữ liệu doanh thu.')
    st.divider()

    col_control, col_status = st.columns([1, 3], gap="large")
    with col_control:
        st.subheader('Bảng điều khiển')
        st.info("""
            **Luồng thực thi:**
            1. Load dữ liệu từ Drive
            2. Làm sạch thông minh (Smart Pipeline)
            3. Ẩn danh khách hàng & mã hóa sản phẩm
            4. Giả lập lịch sử giá & phân bổ thanh toán
            5. Tích hợp Traffic & hoàn thiện Master Data
        """)
        button_col, logs_col = st.columns(2, gap='small')
        run_pipeline = button_col.button('**🚀 Run Pipe Run**', type='secondary', width='stretch')
        logs = logs_col.empty()

    # Xử lý Logic khi nhấn nút
    if run_pipeline:
        config = get_config()
        log_stream = io.StringIO()
        
        with col_status:
            st.subheader('Trạng thái hệ thống')
            with st.status('Đang xử lý dữ liệu...', expanded=True) as status_box:
                with redirect_stdout(log_stream):

                    # Bước 1
                    st.write('**Bước 1:** Đang load dữ liệu từ file CSV...')
                    st.write(f'&nbsp;&nbsp;&nbsp;&nbsp;*Đã load {len(raw_df):,} dòng dữ liệu.*')

                    # Bước 2
                    st.write('**Bước 2:** Đang chuẩn hóa dữ liệu (Smart Data Pipeline)...')
                    df_cleaned = raw_df.pipe(smart_clean_module, config=config)

                    # Bước 3
                    st.write('**Bước 3:** Đang ẩn danh hóa dữ liệu (Anonymization)...')
                    cust_salt = st.secrets['env'].get('CUST_SALT_KEY', 123)
                    df_anon = (df_cleaned
                        .pipe(anonymize_customer_pii, config=config, cust_salt=cust_salt)
                        .pipe(insert_hash_sku_imei, cust_salt=cust_salt, anonym_path=anonym)
                        .pipe(anonymize_sales_product, anonym_path=anonym)
                    )
                    st.write(f'&nbsp;&nbsp;&nbsp;&nbsp;*Đã ẩn danh thông tin khách hàng.*')
                    st.write(f'&nbsp;&nbsp;&nbsp;&nbsp;*Đã ẩn danh thông tin sản phẩm.*')

                    # Bước 4
                    st.write('**Bước 4:** Đang giả lập lịch sử giá và phân bổ thanh toán...')
                    df_mimic = df_anon.pipe(mimic_price_history_and_payments, config=config)


                    # Bước 5
                    st.write('**Bước 5:** Tích hợp dữ liệu Traffic và điền khuyết...')
                    df_final: pd.DataFrame = (df_mimic
                        .pipe(reset_invoice_no)
                        .pipe(ready_order, trash_cols=['ean'], prefer_order=['date', 'invoice', 'product_name', 'revenue'])
                        .pipe(staff_rename)
                        .pipe(join_sales_traffic, traffic_path=traffic)
                        .pipe(bf_fill, _anchor='invoice', _target_cols=['id', 'name', 'email'])
                    )
                
                status_box.update(label='✅ Hoàn tất quy trình!', state='complete', expanded=True)

        with logs.popover('**📝 Pipeline Flow & Logs**'):
            st.subheader('Console Logs')
            st.code(log_stream.getvalue(), language='text')

        
        # Table 1: Before cleaning
        st.subheader('Raw Dummy Sales Data')
        raw_order = [
            'date', 'invoice', 'sa', 'ean', 'cat', 'imei_sn',
            'price', 'qty', 'ins_stt', 'ins_fee', 'disc_percent',
            'disc_amount', 'revenue', 'cash', 'card', 'payoo', 'banking', 'mkt',
            'vnpay', 'trade_in', 'email', 'name', 'phone', 'time', 'fill_date']
        ean_salt = st.secrets['env'].get('EAN_SALT_KEY', 123)
        raw_df['ean'] = raw_df['ean'] + ean_salt
        st.dataframe(raw_df[raw_order], placeholder='-')
        
        
        st.divider()
        
        # Table 2: After cleaning
        st.subheader('Cleaned Dummy Sales Data')
        final_order = [
            'date', 'invoice', 'staff', 'sku', 'cat', 'imei_sn',
            'price', 'qty', 'ins_stt', 'ins_fee', 'disc_percent', 'disc_amount',
            'revenue', 'cash', 'card', 'qr_code', 'id', 'name',
            'memory_size', 'time']
        st.dataframe(df_final.dropna(axis=1, how='all')[final_order], placeholder='-')


    else:
        with col_status:
            st.subheader('Trạng thái hệ thống')
            st.caption(':material/south_west: Vui lòng nhấn nút để khởi chạy.')

if __name__ == '__main__':
    st.set_page_config(page_title="Sales Pipeline", layout="wide") # Mở rộng layout tùy chọn
    sales_pipeline_demo()