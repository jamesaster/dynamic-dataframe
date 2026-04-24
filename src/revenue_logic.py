import pandas as pd
import numpy as np

# --- Nhóm Revenue ---      
def cal_revenue(df: pd.DataFrame, payment_cols: list=None, disc_cols: list=None, mode: str='do_not_cal', results: dict=None)-> pd.Series:
    """
    Parameters:
        payment_cols: Columns containing payment method values
        disc_cols: Columns for payment discounts (unit: %, value, or both)

    ### Execution Modes:
        - 'do_not_cal': Used to bypass Stage_0/1 and check conditions where 
                        count(qty_col) and count(price_col) == 1.
        - 'something_else': Returns replacement revenue.
                            Priority: sum(payments). If no payments: price * qty.
    """
            #    Attribute trong pandas (ghi nhớ concept)
            #  - Nên là bước cuối, tránh các thao tác .loc làm mất attrs
            #  - ghi kèm metadata theo pandas object, trả về 1 dict có thể bao gồm ( dict, list, bool,....)
            #  +   Ghi vào:  obj.attrs['tên_biến'] = giá_trị
            #  +   Lấy ra:   obj.attrs['tên_biến']
            #  +   Xóa sạch: obj.attrs.clear()

    if True: #* Tự động phân loại và xử lý discount_cols (Nếu có)
        disc_percent = None
        disc_amount = None
        if disc_cols and len(disc_cols) <= 2:
            df[disc_cols] = df[disc_cols].apply(pd.to_numeric, errors='coerce')
            # Tính mean() để xác định loại discount
            for col_name in disc_cols:
                mean_disc = df[col_name].mean()
                # Lưu ý np.nan != np.nan Nên phải dùng *pd.isna()
                if pd.isna(mean_disc): continue
                if mean_disc <= 1: 
                    disc_percent = col_name
                else:  disc_amount = col_name
            print(f"DEBUG [cal_revenue]: percent_col={disc_percent}, amount_col={disc_amount}")

    pct_discount = df[disc_percent].fillna(0) if disc_percent else 0
    amt_discount = df[disc_amount].fillna(0) if disc_amount else 0

    if True: #* Tìm cột qty
        qty_rules = {'qty', 'quantity', 'sl', 'so_luong'}
        list_qty = df.columns[[any(word in qty_rules for word in col.split('_')) or col in qty_rules for col in df.columns]]
    if True: #* Extract cột price từ Result, tạo điều kiện *price_n_qty_equal_1
        list_price = results.get('price', [])
        price_n_qty_equal_1 = len(list_price) == 1 and len(list_qty) == 1
        print(f"DEBUG [cal_revenue]: price={list_price}, qty={list_qty}")
        print(f"DEBUG [cal_revenue]: price_n_qty_equal_1 {price_n_qty_equal_1}") #!!!
    
    # Khởi tạo Revenue_Alternate empty  
    #   Tránh lỗi UnboundLocalError trong IF
    rev_alternate = pd.Series(np.nan, index=df.index)
    # Default mode = bỏ qua tính rev_alternate
    if mode != 'do_not_cal':
        if payment_cols:
            pay = df[payment_cols].apply(pd.to_numeric, errors='coerce').sum(axis=1)
            rev_alternate = pay.copy()
        # Fall_back
        if price_n_qty_equal_1:
            p_q = (df[list_price[0]] * df[list_qty[0]]) * (1 - pct_discount) - amt_discount
            
            print(f"DEBUG [cal_revenue]: p_q.mean= {p_q.mean()}")

            pq_mask = (rev_alternate.isna() | rev_alternate == 0)
            if not payment_cols:
                pq_mask[:] = True

            print(f"DEBUG [cal_revenue]: Fall_back price*qty mask={pq_mask.sum()}")
            
            rev_alternate.loc[pq_mask] = p_q.loc[pq_mask]
    # attrs phải để cuối cùng chứ cho lên trước .loc là mất hết
    rev_alternate.attrs['results'] = results
    rev_alternate.attrs['price_n_qty_equal_1'] = price_n_qty_equal_1
    return rev_alternate

#!  rev_validate: main
def rev_validate(df, payment_cols=None, disc_cols: list=None, results: dict=None)-> pd.DataFrame:
    """
    ### kiểm tra số lượng & chất lượng của cột Revenue
        Nếu rev_col == 0: Tạo df[rev] = cal_rev()   
        Nếu rev_col == 1: nếu data.notna() >= 99% -> Tính mean(rev) so sánh mean(payments) | mean(price*qty)
        nếu data.notna() < 99% -> fillna() = cal_rev()
    """
    mask = pd.Series(True, index=df.index)
    rev_alternate = cal_revenue(df, payment_cols, disc_cols, mode='_cal', results=results)
    price_n_qty_equal_1 = rev_alternate.attrs.get('price_n_qty_equal_1', False)

    key = 'revenue'
    list_rev = results.get(key, [])

    if len(list_rev) == 0:
        rev = key
        if rev not in df.columns:
            df[rev] = rev_alternate
            print(f"DEBUG [rev_validate] Created new column: {rev}")

    elif len(list_rev) == 1:
        rev = list_rev[0]
        df[rev] = pd.to_numeric(df[rev], errors='coerce')
        rev_na = df[rev].isna().sum()
        rev_val = df[rev].notna().mean()
        print(f'DEBUG [rev_validate] Detected: 1 revenue column ({rev_val*100:.2f}% valid)')
        
        if rev_val < 0.99:

            if payment_cols or price_n_qty_equal_1:
                #! Pandas DataFrame có thể ở trạng thái View hoặc Copy tùy vào thao tác trước đó | No Guarantee |
                #! Bài học rút ra sau 10 tiếng Debug: luôn tạo copy col liên quan trước khi tạo mask
                col = df[rev].copy()   # 🔥 ép ổn định snapshot
                mask = col.isna() | (col == 0)

                print(f"DEBUG NaN: {df[rev].isna().sum()}, mask: {(df[rev].isna() | (df[rev]==0)).sum()}")
                print(f"{'DEBUG [rev_validate] rev_alternate.notna() count:':<50} {rev_alternate.notna().sum()}")
                print(f"{'DEBUG [rev_validate] Index match ?:':<50} {df.index.equals(rev_alternate.index)}")
                print(f"{'DEBUG [rev_validate] rev_alternate[mask].isna():':<50} {rev_alternate.loc[mask].isna().sum()} 👀")
                print(f"{'DEBUG [rev_validate] rev.isna() | (rev == 0) True:':<50} {mask.sum()} <==")
                df.loc[mask, rev] = rev_alternate.loc[mask]
                print(f"{'DEBUG [rev_validate] Revenue gaps filled:':<50} {mask.sum() - df[rev].isna().sum()} <== Final Result")
                print(f"{'DEBUG [rev_validate] <N/a> count after filled:':<50} {df[rev].isna().sum()} 👀")
            else:
                print(f'DEBUG [rev_validate] Insufficient data to fill in {rev_na} revenue gaps.')

        # Tính mean để đối chiếu (reset mask về True để tính mean toàn cột)
        current_rev_numeric = pd.to_numeric(df[rev], errors='coerce')
        rev_mean = current_rev_numeric.mean()
        rev_alter_mean = rev_alternate.mean()

        if (payment_cols or price_n_qty_equal_1) and pd.notna(rev_alter_mean) and rev_alter_mean != 0:
            print(f'DEBUG [rev_validate] revenue(mean) / alter_revenue(mean) = {(rev_mean/rev_alter_mean)*100:.2f}%')
            
    else:
        print(f"DEBUG [rev_validate] Cảnh báo: Có nhiều cột {key}: {list_rev}")
        
    return df
