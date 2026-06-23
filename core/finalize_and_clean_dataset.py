


# 9. Reset invoices number | Clean up -------------------------------------------------


_orders = ['date', 'invoice', 'sa', 'sku', 'imei_sn', 'cat', 
        'detail_sub_lob', 'product_name', 'price', 'qty', 
        'ins_stt', 'ins_fee', 'disc_percent', 'disc_amount', 
        'revenue', 'cash', 'card', 'qr_code']
trash_cols = ['ean', 'fill_date', 'no_payment']

# df_ready = (df_ready
#     .pipe(reset_invoice_no)
#     .pipe(ready_order, trash_cols, _orders)
#     .pipe(staff_rename)
# )




# df_ready.to_parquet(r'../CSV_read_only/APPLE_2024_2025_PIPE_RESULT.parquet', compression='snappy', index=False)