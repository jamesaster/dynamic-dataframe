
def get_columns(df):    
    cols = df.columns
    max_len = max(len(col) for col in cols)
    print("class colName:")
    for col in cols:
        print(f"    {col:<{max_len}} = '{col}'")

class colRaw:
    time           = 'time'
    date           = 'date'
    week           = 'week'
    month          = 'month_year'

    invoice        = 'invoice'
    staff          = 'staff'
    ean            = 'ean'
    sku            = 'sku'
    prod_name      = 'product_name'
    imei_sn        = 'imei_sn'
    cat            = 'cat'
    subcat         = 'detail_sub_lob'

    qty            = 'qty'
    price          = 'price'
    revenue        = 'revenue'

    disc_pct       = 'disc_percent'
    disc_amt       = 'disc_amount' 

    cash           = 'cash'
    card           = 'card'
    payoo          = 'payoo'
    banking        = 'banking'
    mkt            = 'mkt_promo'
    vnpay          = 'vnpay'
    trade_in       = 'trade_in'

    cus_id         = 'id'
    cus_name       = 'name'
    cus_email      = 'email'
    traffic        = 'date_traffic'
    event_name     = 'event_name'
# colRaw_mapping = {
#     0: colRaw.date,
#     1: colRaw.invoice,
#     2: colRaw.staff,
#     3: colRaw.ean,
#     5: colRaw.cat,
#     6: colRaw.imei_sn,
#     7: colRaw.sku,
#     8: colRaw.prod_name,
#     9: colRaw.price,
#     10: colRaw.qty,
#     # 11: "ins_stt", Ignore
#     # 12: "ins_fee", Ignore
#     13: colRaw.disc_pct,
#     14: colRaw.disc_amt,
#     15: colRaw.revenue,
#     16: colRaw.cash,
#     17: colRaw.card,
#     18: colRaw.payoo,
#     19: colRaw.banking,
#     20: colRaw.mkt,
#     21: colRaw.vnpay,
#     22: colRaw.trade_in,
#     # 23: "vat", Ignore
#     24: colRaw.cus_email,
#     25: colRaw.cus_name,
#     26: colRaw.cus_id,
#     # 27: "note", Ignore
#     28: colRaw.time
# }
colRaw_mapping = {
    0: colRaw.date,
    1: colRaw.invoice,
    2: colRaw.staff,
    3: colRaw.ean,
    4: colRaw.cat,
    5: colRaw.imei_sn,
    6: colRaw.sku,
    7: colRaw.prod_name,
    8: colRaw.price,
    9: colRaw.qty,
    10: colRaw.disc_pct,
    11: colRaw.disc_amt,
    12: colRaw.revenue,
    13: colRaw.cash,
    14: colRaw.card,
    15: colRaw.payoo,
    16: colRaw.banking,
    17: colRaw.mkt,
    18: colRaw.vnpay,
    19: colRaw.trade_in,
    20: colRaw.cus_email,
    21: colRaw.cus_name,
    22: colRaw.cus_id,
    23: colRaw.time
}


class colName:
    time           = 'time'
    date           = 'date'
    week           = 'week'
    month          = 'month_year'
    invoice        = 'invoice'
    staff          = 'staff'
    sku            = 'sku'
    prod_name      = 'product_name'
    imei_sn        = 'imei_sn'
    cat            = 'cat'
    subcat         = 'detail_sub_lob'
    price          = 'price'
    qty            = 'qty'
    revenue        = 'revenue'
    pay_cash       = 'cash'
    pay_card       = 'card'
    pay_qr         = 'qr_code'
    cus_id         = 'id'
    cus_name       = 'name'
    cus_email      = 'email'
    traffic        = 'date_traffic'
    event_name     = 'event_name'

class stockCol:
    date           = 'date'
    prod_name      = 'product_name'
    lot            = 'lot'
    start          = 'start'
    import_po      = 'import_po'
    import_do      = 'import_do'
    stock_take     = 'stock_take'
    transfer       = 'transfer'
    noname_1       = 'noname_1'
    noname_2       = 'noname_2'
    sell           = 'sell'
    returns        = 'return'
    rtv            = 'rtv'
    noname_3       = 'noname_3'
    noname_4       = 'noname_4'
    end            = 'end'
    sku            = 'sku'
    cat            = 'cat'
    subcat         = 'detail_sub_lob'
    price          = 'price'
