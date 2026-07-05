# 1 - Import thư viện và module cần thiết
from src import reset_invoice_no, ready_order, staff_rename
from core import (
    load_and_normalize,     smart_sales_clean, 
    anonymize_customer_pii, process_product_master,
    insert_hash_sku_imei,   anonymize_sales_product, 
    mimic_price_history_and_payments, 
    join_sales_traffic,     bf_fill)
from contextlib import redirect_stdout
from dotenv import load_dotenv
from pathlib import Path
import streamlit as st
import pandas as pd
import numpy as np
import json
import sys
import io
import os

@st.cache_data(show_spinner="Đang truy xuất dữ liệu...")
def run_forest_run()-> pd.DataFrame:
    """
    PIPELINE XỬ LÝ & ẨN DANH DATA RETAIL
    --------------------------------------
    1. Clean & Sync: Gộp CSV 2024-2025, chuẩn hóa cột & format.
    2. Security: Hash PII (Phone/Email) bằng Salt, giấu IMEI/SN.
    3. Finance Logic: Giả lập Payment, Price History & scaling giá.
    4. Finalize: Join Traffic, Backfill data & sắp xếp cột ready-to-use.

    Input: CSVs Raw, SECRET.env, DYNAMIC_PRICE.json
    Output: df_master (Clean - Anonymous - Ready for BI)
    """

    BASE_DIR = Path(__file__).parent
    f = io.StringIO()

    # 2 - Lấy Salt và Random Seed từ file .env
    load_dotenv(dotenv_path=BASE_DIR / 'config' / 'SECRET.env')
    if os.getenv('CUST_SALT_KEY') is None:
        print("Vui lòng kiểm tra file SECRET.env")
        exit(1)
    cust_salt = int(os.getenv('CUST_SALT_KEY'))
    prod_salt = int(os.getenv('PRODUCT_SALT_KEY'))
    ran_seed  = int(os.getenv('RANDOM_SEED'))
    traffic_scale = [float(x) for x in os.getenv('TRAFFIC_SCALE').split(",")]

    # 3 - Load JSON scale_dict cho price scaling
    try:
        with open(BASE_DIR / 'config' / 'DYNAMIC_PRICE.json', 'r') as js:
            scale_dict = json.load(js)
    except Exception as e:
        print(f"Vui lòng kiểm tra file DYNAMIC_PRICE.json: {e}")
        exit(1)

    # 4 - Path đến source CSV và config xử lý
    csv_2024     = BASE_DIR / 'CSV_read_only' / 'SALES_APPLE_2024_CLEAN.csv'
    csv_2025     = BASE_DIR / 'CSV_read_only' / 'SALES_APPLE_2025_DIRTY.csv'

    product_info = BASE_DIR / 'CSV_read_only' / 'UPDATED_ALL_PRICE_JAMES.csv'
    anonym_price = BASE_DIR / 'data_output'   / 'Anonym_Price.csv'
    traffic_path = BASE_DIR / 'CSV_read_only' / 'APPLE_2024_2026_FAKE_TRAFFIC.parquet'

        # config chung cho pipeline
    config = {
        'payment_cols': ['cash', 'card', 'payoo', 'banking', 'mkt', 'vnpay', 'trade_in'],
        'disc_cols'   : ['disc_percent', 'disc_amount'],
        'date_anchor' : 'invoice',
        'date_pocket' : 'date',
        'drop_col'    : ['vat', 'note'],
        'anonymous'   : ['phone', 'name', 'email',]
        }

        # Tham số cho hàm ready_order(), cleaning và sắp xếp lại cột cho df_ready
    _orders = ['date', 'invoice', 'sa', 'sku', 'imei_sn', 'cat', 
            'detail_sub_lob', 'product_name', 'price', 'qty', 
            'ins_stt', 'ins_fee', 'disc_percent', 'disc_amount', 
            'revenue', 'cash', 'card', 'qr_code']
    trash_cols = ['ean', 'fill_date', 'no_payment', 'ins_stt', 'ins_fee', 'disc_percent', 'disc_amount', 'color']
        

    # product_master = process_product_master( # NOTE: Hàm này có thể chạy độc lập nếu chỉ cần xử lý product master, export CSV nếu muốn
    #     product_info=product_info, 
    #     prod_salt=prod_salt, 
    #     ran_seed=ran_seed, 
    #     scale_dict=scale_dict,
    #     export_csv_path=anonym_price) # anonym_price 

    # 5 - Chạy pipeline xử lý dữ liệu -> df_master
    with redirect_stdout(f):

        # NOTE 
        raw_df = load_and_normalize(csv_2024, csv_2025, config)
        df = (raw_df
            .pipe(smart_sales_clean, 
                config=config)

            .pipe(anonymize_customer_pii, 
                config=config, 
                cust_salt=cust_salt)

            .pipe(insert_hash_sku_imei, 
                cust_salt=cust_salt, 
                anonym_path=anonym_price)

            .pipe(anonymize_sales_product, 
                anonym_path=anonym_price)

            .pipe(mimic_price_history_and_payments, 
                config=config)
        )

        # NOTE 
        df_ready = (df
            .pipe(reset_invoice_no)

            .pipe(ready_order, 
                trash_cols=trash_cols, 
                prefer_order=_orders)

            .pipe(staff_rename)
        )

        _cust_anc = 'invoice'
        _cust_cols = ['id', 'name', 'email']

        # NOTE 
        df_master = (df_ready
            .pipe(join_sales_traffic, 
                traffic_path=traffic_path)
            .pipe(bf_fill, 
                _anchor=_cust_anc, 
                _target_cols=_cust_cols))
        

    assert isinstance(df_master, pd.DataFrame), 'ép hiện docstring cho df nếu lỗi'
    return df_master

    

if __name__ == "__main__":
    test = run_forest_run()
    print(test.columns)