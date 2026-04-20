import pandas as pd
import numpy as np

# --- Nhóm Revenue ---      
def cal_rev(df: pd.DataFrame, mask: pd.Series, payment_cols: list=None, mode: str='do_not_cal', results: dict=None)-> pd.Series:
    # def có 2 mode:
    #    - 'do_not_cal': chỉ mượn hàm để passing Dictionary results và Conditions count(qty_col) and count(price_col) == 1 
    #    - 'something_else': trả về revenue thay thế, ưu tiên sum(payments) , if not payments: price * qty
    #* Attribute trong pandas (ghi nhớ concept)
    #  - ghi kèm metadata theo pandas object, trả về 1 dict có thể bao gồm ( dict, list, bool,....)
    # Ghi vào: obj.attrs['tên_biến'] = giá_trị
    # Lấy ra: print(obj.attrs['tên_biến'])
    # Xóa sạch: obj.attrs.clear()

    # Nhận results từ tham số truyền vào để tránh gọi lại stage_1 gây lỗi unpack
    qty_rules = {'qty', 'quantity', 'sl', 'so_luong'}
    list_price = results.get('price', [])

    list_qty = df.columns[[any(word in qty_rules for word in col.split('_')) or col in qty_rules for col in df.columns]]
    price_n_qty_equal_1 = len(list_price) == 1 and len(list_qty) == 1

    # Khởi tạo biến rev_alter empty và same ở if -> elif -> return (1 flow cover all cases) Tránh lỗi UnboundLocalError.
    rev_alter = pd.Series(np.nan, index=df.index)
    # mode default là bỏ qua tính rev_alter
    if mode != 'do_not_cal':
        if payment_cols:
            rev_alter = df.loc[mask, payment_cols].apply(pd.to_numeric, errors='coerce').sum(axis=1)
        elif price_n_qty_equal_1:
            rev_alter = df.loc[mask, list_price[0]] * df.loc[mask, list_qty[0]]
            
    # Ghi kèm hướng dẫn sd, phải để cuối cùng chứ cho lên .loc là mất hết
    rev_alter.attrs['results'] = results
    rev_alter.attrs['price_n_qty_equal_1'] = price_n_qty_equal_1
    return rev_alter

#!  rev_validate: main
def rev_validate(df, payment_cols=None, results: dict=None)-> pd.DataFrame:
    #* def kiểm tra số lượng & chất lượng của cột Revenue
    #* Nếu rev_col == 0: Tạo df[rev] = cal_rev()
    #* Nếu rev_col == 1: nếu data.notna() >= 99% -> Tính mean(rev) so sánh mean(payments) | mean(price*qty)
    #* nếu data.notna() < 99% -> fillna() = cal_rev()

    mask = pd.Series(True, index=df.index)
    cal_results = cal_rev(df, mask, payment_cols, mode='do_not_cal', results=results)
    price_n_qty_equal_1 = cal_results.attrs.get('price_n_qty_equal_1', False)

    key = 'revenue'
    list_rev = results.get(key, [])

    if len(list_rev) == 0:
        rev = key
        if rev not in df.columns:
            df[rev] = cal_rev(df, mask, payment_cols, mode='cal', results=results)
            print(f"[rev_validate] Created new column: {rev}")

    elif len(list_rev) == 1:
        rev = list_rev[0]
        rev_val = pd.to_numeric(df[rev], errors='coerce').notna().mean()
        rev_na = df[rev].isna().sum()
        print(f'[rev_validate] Detected: 1 revenue column ({rev_val*100:.2f}% valid)')
        
        # Tính toán giá trị thay thế cho toàn bộ bảng để dùng chung
        rev_alter_val = cal_rev(df, mask, payment_cols, mode='cal', results=results)
        
        if rev_val < 0.99:
            if payment_cols or price_n_qty_equal_1:
                mask = df[rev].isna()
                df.loc[mask, rev] = rev_alter_val.loc[mask]
                print(f'[rev_validate] Revenue gaps: {rev_na} filled')
            else:
                print(f'[rev_validate] Insufficient data to fill in {rev_na} revenue gaps.')

        # Tính mean để đối chiếu (reset mask về True để tính mean toàn cột)
        current_rev_numeric = pd.to_numeric(df[rev], errors='coerce')
        rev_mean = current_rev_numeric.mean()
        
        rev_alter_mean = rev_alter_val.mean()
        if (payment_cols or price_n_qty_equal_1) and rev_alter_mean != 0:
            print(f'[rev_validate] revenue(mean) / alter_revenue(mean) = {(rev_mean/rev_alter_mean)*100:.2f}%')
            
    else:
        print(f"[rev_validate] Cảnh báo: Có nhiều cột {key}: {list_rev}")
        
    return df
