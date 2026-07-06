import pandas as pd
from src.datetime_logic import time_format
from src.utils import is_boolean, is_datetime, is_alo, is_money, is_numeric, is_category

def stage_0(df: pd.DataFrame):
    #! stage_0: CAT_BY_NAME
    rules = {
    'date_col'    : r'^(?!.*traffic)(?:^|_)(?:date|ngay|day|created|updated)(?:_|$)',
    'time_col'    : r'(?:^|_)(?:time|gio|hour|minute|second|timestamp)(?:_|$)',
    'price'       : r'(?:^|_)(?:price|pice|unit_price|unitprice|đơn_giá|đơn_gia|gia|giá|gia_ban|giá_bán|prc)(?:_|$)',
    'numeric_col' : r'(?:^|_)(?:cash|card|vnpay|payoo|trade_in|banking|mkt_promo|cost|qty|quantity|sl|disc|discount|percent|fee|rate|tax|shipping)(?:_|$)',
    'revenue'     : (r'(?:^|_)(?:revenue|total|total_amount|total_revenue|thanh_tien|thanhtien|'
                    r'doanh_thu|doanhthu|tổng_tiền|tong_tien|tongtien|grand_total|subtotal|tt|'
                    r'(?<!disc_)(?<!tax_)(?<!fee_)(?<!paid_)(?<!ship_)amount)(?:_|$)' ),
    'boolean_col' : r'(?:^|_)(?:ins_stt|tg|is_|status|active)(?:_|$)',
    'category_col': r'(?:^|_)(?:cat|type|category)(?:_|$)',
    'string_col'  : (r'(?:^|_)(?:ean|invoice|inv(?:oice|_no|_number)?|order_id|transaction_id|'
                    r'bill_no|bill_number|ma_hoa_don|serial|sku|upc|code|id)(?:_|$)' )
            }

    results = {key: [] for key in rules} | {key: [] for key in ['phone_col']}
    colname = df.columns.astype('string')

    #* all_true phát 1 vé True cho mỗi cột trong df.columns
    #*  - 
    # Each column only has one True ticket.
    all_true = pd.Series([True] * len(colname))
    
    for pocket, regex in rules.items():
        # Tạo mask: str.contains() trả True/False khi tên cột CHỨA item[1] rule
        boole = colname.str.strip().str.contains(regex, case=False, na=False, regex=True)
        # Update the True from boole mask with the False from all_true
        boole = boole & all_true
        # If not convert result to_list(), it wwould return like: 'date_col': Index(['date'], dtype='string')
        results[pocket] = colname[boole].to_list()
        # ~boole = match equal False
        all_true = all_true & ~boole
        
    #* Cái gì đi qua loop cũng chả còn vẹn nguyên :)
    # all_true down here is not all_true anymore
    pending_cols = colname[all_true].to_list()

    print(f'[Stage_0] Detected: {len(colname)} columns')
    print(f'[Stage_0] Matched: {sum(len(v) for v in results.values())} columns')
    print(f'[Stage_0] Passing to [Stage_1]: {len(pending_cols)} columns')

    return df, results, pending_cols   
def stage_1(output_stage_0)-> dict:
    #! stage_1: CAT_BY_SAMPLE_DATA
    pocket_func = {
        'phone_col'   : [is_alo],
        'date_col'    : [is_datetime],
        'boolean_col' : [is_boolean],
        'numeric_col' : [is_money, is_numeric],
        'category_col': [is_category] 
    }
    #! unpack các biến từ stage_0
    df, results, pending_cols = output_stage_0

    # Chỉ xử lý các cột pending từ stage_0
    # flag ở đây là gì chả quan trọng vì flag reset mỗi loop
    for col in pending_cols:
        # series = get_sample(df[col])
        series = df[col].head(1000).dropna().head(500)
        flag = False
        for pocket, func in pocket_func.items():
            # print(f'{col} >>> tới "{pocket}"')
            for f in func:
                # print(f'{col} thử {f.__name__}')
                if f(series):
                    results[pocket].append(col)
                    # print(f"+ Nhập '{col}' vào: {pocket} vì khớp {f.__name__} flag = True")
                    flag = True
                    break
            if flag:
                break
            
        if not flag:
            results['string_col'].append(col)

    print(f'[Stage_1] Result: {[f"{pocket}: {len(cols)}" for pocket, cols in results.items()]}')
    print(f'[Stage_1] Others: {results.get("string_col", [])} > String')
    print('\n')
    return results
def execution(df: pd.DataFrame, final_results: dict)-> pd.DataFrame:
    df_new = pd.DataFrame(index=df.index)

    for key, cols in final_results.items():
        if not cols:
            continue

        if key == 'time_col':
            for c in cols:
                df_new[c] = time_format(df, c)
        if key == 'date_col':
            df_new[cols] = df[cols]      
        # Update 28-06-26: Đã pd.to_numeric[price, revenue, numeric_col] trước khi vào hàm
        if key == 'price':
            df_new[cols] = df[cols]
        if key == 'revenue':
            df_new[cols] = df[cols]
        if key == 'numeric_col':
            df_new[cols] = df[cols].fillna(0)
        if key == 'boolean_col':
            df_new[cols] = df[cols].astype('boolean')
        if key == 'category_col':
            continue

    str_keys = ['string_col', 'phone_col', 'category_col']
    str_cols = sum([final_results.get(key, []) for key in str_keys], [])
    if str_cols:
        df_new[str_cols] = df[str_cols].replace(r'^\s*$', '-', regex=True).astype('string')

    try:
        df_new = df_new[df.columns]
    except:
        missing_cols = set(df.columns) - set(df_new.columns)
        print(f'🫠 Execution > Thiếu cột: {missing_cols}')
        if len(missing_cols) == 1 and 'fill_date' in missing_cols:
            print(f'🤷‍♂️ Kệ {missing_cols}')
        
        existing_cols = [c for c in df.columns if c in df_new.columns]
        df_new = df_new[existing_cols]
    return df_new

