import pandas as pd
import numpy as np 

def is_boolean(sample: pd.Series)-> bool:
    """### Một cột được coi là boolean nếu nó chứa các giá trị có định dạng giống boolean phổ biến, bao gồm các giá trị như 'true', 'false', 'yes', 'no', '1', '0' (không phân biệt chữ hoa chữ thường), và có tỷ lệ phần trăm lớn các giá trị tuân theo định dạng này."""
    if sample.empty: return False
    if sample.dtype == 'bool': return True
    s = sample.astype('string').str.strip().str.lower()
    x = s.isin(['true','false','yes','no','1','0']).mean()
    y = s.nunique() 
    return (x >= 0.9) and (y <= 2)

def is_datetime(sample: pd.Series)-> bool:
    """### Một cột được coi là datetime nếu nó chứa các giá trị có định dạng giống ngày tháng hoặc thời gian phổ biến, và có tỷ lệ phần trăm lớn các giá trị tuân theo định dạng này."""
    if sample.empty: return False
    if sample.dtype.kind in ['b','i','u','f']: return False
    if sample.dtype.kind == 'M': return True
    x = pd.to_datetime(sample, format='mixed', errors='coerce').notna().mean()
    y = sample.str.contains(r'[-/: ]', na=False).mean()
    return (x >= 0.9) and (y >= 0.9)

def is_alo(sample: pd.Series)-> bool:
    """### Một cột được coi là số điện thoại nếu nó chứa các giá trị có định dạng giống số điện thoại phổ biến, bao gồm các ký tự như dấu gạch nối, dấu cách, hoặc ngoặc đơn, và có tỷ lệ phần trăm lớn các giá trị tuân theo định dạng này."""
    if sample.empty: return False
    s = sample.dropna().astype('string')
    x = s.str[0].isin(['0','3','5','7','8','9']).mean()
    y = s.str.replace(r'[,\-\s]', '', regex=True).str.len().mean()
    return (x >= 0.9) and (9 <= y <= 11)

def is_money(sample: pd.Series)-> bool:
    """### Một cột được coi là tiền tệ nếu nó chứa các giá trị có định dạng giống số tiền phổ biến, bao gồm các ký tự như dấu gạch nối, dấu cách, dấu phẩy, hoặc dấu chấm thập phân, và có tỷ lệ phần trăm lớn các giá trị tuân theo định dạng này."""
    if sample.empty: return False
    if not sample.dtype.kind == 'o': return False
    pattern = r'^\s*-?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*$'
    return sample.astype('string').str.strip().str.match(pattern, na=False).mean() >= 0.9

def is_numeric(sample: pd.Series)-> bool:
    """### A column is considered numeric if it can be converted to numeric values with a high success rate."""
    if sample.empty: return False
    return pd.to_numeric(sample, errors='coerce').notna().mean() >= 0.9

def is_category(sample: pd.Series)-> bool:
    """### A column is considered categorical if it has a low ratio of unique values to total values, and is not predominantly numeric."""
    if sample.empty: return False
    numeric_ratio = pd.to_numeric(sample, errors='coerce').notna().mean()
    nunique = sample.nunique()

    if numeric_ratio > 0.8:
        return False
    return 2 <= nunique <= 33

def fill_origin_price(df: pd.DataFrame, col_map: dict = None, price_ratio: bool = False
) -> pd.DataFrame:
    """
    ### Tính toán giá gốc dựa trên `Revenue`, `Price`, `Qty`, `Discount` methods.
    ### Tạo `price_ratio` cho từng row (price_ratio=True)
        - default_cols if col_map = None:
            default_cols = {
            'revenue': 'revenue',
            'disc_amount': 'disc_amount',
            'disc_percent': 'disc_percent',
            'qty': 'qty',
            'price': 'price',
            'ean': 'ean'
            }
    """

    default_cols = {
        'revenue': 'revenue',
        'disc_amount': 'disc_amount',
        'disc_percent': 'disc_percent',
        'qty': 'qty',
        'price': 'price',
        'ean': 'ean'
    }

    cols = default_cols | (col_map or {})

    df = df.copy()

    # calculate origin price
    origin = (
        (df[cols['revenue']] + df[cols['disc_amount']]) /
        ((1 - df[cols['disc_percent']]) * df[cols['qty']])
    ).replace([np.inf, -np.inf], np.nan).fillna(df[cols['price']])

    # Create price_dict ( original price for each EAN )
    price_dict = (
        origin
        # Thao tác trên Series không set_index() được nên dùng set_axis() hoặc s.index() nếu không cần chain
        .set_axis(df[cols['ean']])
        .groupby(level=0)
        .first()
        .replace([np.inf, -np.inf], np.nan)
        .rename(cols['price'])
        .reset_index()
        .dropna()
    )
    # Astype for lookup
    price_dict[cols['ean']] = (pd.to_numeric(price_dict[cols['ean']], errors='coerce').astype('Int64').astype('string'))

    # Groupby 1 lần nữa sau khi đồng nhất ean
    price_dict = price_dict.groupby(cols['ean']).first() 
    price_dict = price_dict[cols['price']]

    #* Re-creating price col
    raw_price_backup = df[cols['price']].copy().replace([np.inf, -np.inf], np.nan)
    df[cols['price']] = df[cols['ean']].map(price_dict).fillna(raw_price_backup)
    print(f'Debug [fill_origin_price] invalid Re-creating price rows: {df[cols['price']].isna().sum()}')

    if price_ratio:
        first_price = df.groupby(cols['ean'])[cols['price']].transform('first')
        print(f'Debug [fill_origin_price] (1st_price == 0 | missing) count: {((first_price == 0) | (first_price.isna())).sum()}') 
        df['price_ratio'] = first_price / df[cols['price']]

    return df

def reset_invoice_no(df: pd.DataFrame, inv_col: str='invoice'):
    """
    ### Reset invoice number
        Start from 10_000
    """
    inv = pd.to_numeric(df[inv_col], errors='coerce').mask(lambda x: x < 999).ffill()
    first_invoice = inv.dropna().nsmallest(1).values[0] 
    df.loc[:, inv_col] = (inv - first_invoice + 10_000).astype('Int32').astype('string')
    print(f"{f'Debug [reset_invoice_no] Reset invoice number':<50} | Unique count: {df[inv_col].nunique()}")
    return df

def ready_order(df: pd.DataFrame, trash_cols: list=[], prefer_order: list=[]):
    """
    ### Sắp xếp lại thứ tự các cột trong DataFrame
    - `trash_cols` sẽ được loại bỏ khỏi DataFrame.
    - Các cột trong `prefer_order` sẽ được ưu tiên sắp xếp theo thứ tự đã cho.
    - Các cột không có trong `prefer_order` sẽ được sắp xếp sau"""

    ready_cols = sorted([c for c in df.columns if c not in trash_cols], 
                        key=lambda x: prefer_order.index(x) if x in prefer_order else len(prefer_order))
    print(f"{f'Debug [ready_order] Reordered columns':<50} | Final count: {len(ready_cols)}")
    return df[ready_cols]

def staff_rename(df_origin: pd.DataFrame, _staff: str='sa')-> pd.DataFrame:
    """
    ## Tạo cột `staff` (index_base) danh sách nhân viên từ `tên_gốc`
    ### Drop original
    """
    df = df_origin.copy()
    staff_list = sorted(df[_staff].unique())
    staff_dict = pd.Series({staff: f'STAFF_{str(index).zfill(2)}' for index, staff in enumerate(staff_list, start=1)}, name='staff')
    if 'staff' not in df.columns:
        idx = df.columns.get_loc(_staff)
        df.insert(idx, 'staff', df[_staff].map(staff_dict))
        df = df.drop(_staff, axis=1)
    print(f"{f'Debug [staff_rename] Created staff column':<50} | Unique count: {df['staff'].nunique()}")
    return df

def today_hanoi():
    """
    ### Không sợ lệch múi giờ server Streamlit.
    """
    return pd.Timestamp.today(tz='Asia/Ho_Chi_Minh').normalize().tz_localize(None)
