
def get_columns(df):    
    cols = df.columns
    max_len = max(len(col) for col in cols)
    print("class colName:")
    for col in cols:
        print(f"    {col:<{max_len}} = '{col}'")

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
