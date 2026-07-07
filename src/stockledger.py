from pathlib import Path
import streamlit as st
import pandas as pd
import numpy as np
from .columns import stockCol as s
# colNamed

@st.cache_data
def process_stockLedger(path: Path):
    """
    ## Process & clean raw ETP Stock Ledger xls file.
    >>> [Reminder] Both (Raw & Result) is a table with each SKU has 1 row per day (if any change made)
    - Update 6-6-26: added 2 columns = [5, 12]

    This function performs ETL to transform the layout of raw xls 
    into a clean flat table. fixes row-misalignment issue where
    product details and inventory metrics sit on separate rows.

    Processing Steps:
        1. Load required columns and auto-convert data types.
        2. Add a dummy row at the bottom to prevent data loss during shifting.
        3. Shift the 'product' column down by 1 row to align product info
           with its corresponding Lot and quantity metrics.
        4. Extract 'date' from the product column and forward-fill (ffill).
        5. Remove blank rows and system summary rows.
        6. Split the raw product string into separate 'sku' and 'product_name'
        columns.
        7. Drop the original raw product column and return the clean data.

    Parameters
    ----------
    path : Path
        The file path to the raw ETP Stock Ledger Excel file.

    Returns
    -------
    pd.DataFrame
        Final cols: 'date', 'sku',
        'product_name', 'lot', 'start', 'import_po', 'import_do',
        'transfer', 'sell', 'return', 'end'.
    """
    COL_MAP = {
        0: 'product',
        1:  s.lot,
        2:  s.start, #* Start và End luôn bọc 2 đầu numeric
        3:  s.import_po,
        4:  s.import_do,
        5:  s.stock_take, #! update
        6:  s.transfer,
        7:  s.noname_1,  #? Trash col có ảnh hưởng tồn cuối
        8:  s.noname_2,  #? Trash col ...
        9:  s.sell,
        10: s.returns,
        12: s.rtv,      #! update
        14: s.noname_3, #? Trash col ...
        16: s.noname_4, #? Trash col ...
        18: s.end
        }
    if path is None: return
    if isinstance(path, pd.DataFrame):
        RAW_LEDGER = path.iloc[:, list(COL_MAP)].convert_dtypes()
        RAW_LEDGER.columns = list(COL_MAP.values())
    else:
        if path.suffix == '.xls':
            RAW_LEDGER  = pd.read_excel(
                path,
                usecols = COL_MAP.keys(),
                names   = COL_MAP.values()
                ).convert_dtypes()
        elif path.suffix == '.csv':
            RAW_LEDGER  = pd.read_csv(
                path,
                header  = 0,
                usecols = list(COL_MAP.keys())
                ).convert_dtypes()
            RAW_LEDGER.columns = list(COL_MAP.values())
    
    RAW_cols = RAW_LEDGER.columns
    
    #? Tạo 1 dòng buffer
    new_idx = len(RAW_LEDGER)
    stock_ledger = RAW_LEDGER.copy()
    stock_ledger.loc[new_idx, :] = np.nan


    #! Col_0 = Product Info + Date info
    #? Shift Col_0 xuống 1 dòng
    stock_ledger.iloc[:, 0] = stock_ledger.iloc[:, 0].shift(1)
    

    #? Tạo cột ngày từ Col_0
    stock_ledger.insert(0, s.date, pd.to_datetime(stock_ledger.iloc[:, 0], dayfirst=True, errors='coerce'))
    stock_ledger[s.date] = stock_ledger[s.date].ffill()


    #? Clean blank rows
    check_cols = RAW_cols[1:]   # Type 1: Blank without any values, only shows L001 at Col_0
    end_col    = RAW_cols[-1]   # Type 2: System summary rows, no End values

    stock_ledger = stock_ledger.dropna(subset=check_cols, how='all', ignore_index=True)
    stock_ledger = stock_ledger.dropna(subset=[end_col], ignore_index=True)


    #? Fill Product Info
        # now 'L001' only shows where Lot Number exist
    _product = RAW_cols[0]
    stock_ledger[_product] = stock_ledger[_product].replace('L001', np.nan).ffill()


    #? Split Product Info -> SKU & Product_Name
    product_split_df = stock_ledger[_product].str.split(' / ', n = 1, expand=True)
    stock_ledger.insert(1, s.sku, product_split_df[0])
    stock_ledger.insert(2, s.prod_name, product_split_df[1].str.replace(r'(?i)(?<=\d[gt]b).*', '', regex=True))

    #region NOTE [Debug] 31-05-26
        # Mỗi SKU sẽ update 1 record/ngày (nếu ngày đó có thay đổi)
        # Khi Groupby('lot'), nếu lot = None. Pandas sẽ drop row nên cần fillna('-')
    stock_ledger[s.lot] = stock_ledger[s.lot].astype('string').fillna('-')
    #endregion
    final_cols = [col for col in stock_ledger.columns if col != _product]
    final_ledger = stock_ledger.loc[:, final_cols]


    numeric_cols = final_ledger.select_dtypes('number').columns
    final_ledger[numeric_cols] = final_ledger[numeric_cols].apply(pd.to_numeric, downcast='integer')


    return final_ledger

@st.cache_data
def stockLedger_hashed_sku_map(df_ledger: pd.DataFrame, anonym_path: Path):
    anonym_price = pd.read_csv(anonym_path)
    anonym_temp = anonym_price.drop_duplicates(subset='sap_article', ignore_index=True)
    anonym_temp = anonym_temp.rename(columns={'sap_article': s.sku}).set_index(s.sku)['master_sku']
    
    df_product = df_ledger[[s.sku, s.prod_name]].groupby([s.sku, s.prod_name], as_index=False).first().reset_index(drop=True)
    df_product.insert(0, 'master_sku', df_product[s.sku].map(anonym_temp))
    df_product = df_product.set_index(s.sku)['master_sku']

    return df_product

@st.cache_data
def get_inventory_value(
        stock_ledger : pd.DataFrame,
        _date        : str = s.date,
        _price       : str = s.price,
        _last_qty    : str = s.end,
        sku_n_imeisn : list = [s.sku, s.lot]
    )-> pd.Series:
    """
    ### Hàm tạo 2D matrix bằng pivot_table từ `StockLedger` data (cleaned)
        - Cấu trúc trục X: pivot_index = UNIQUE combination of [SKU + LOT Number (empty lot = '-')]
        - Cấu trúc trục Y: reindexed date_range
        - IMPORTANCE: must include data from very first date
        - Mục đích: Trích xuất tổng giá trị tồn kho từ thời điểm bắt đầu đến thời điểm chọn
    """
    REQUIRE_COLS = sku_n_imeisn + [_date, _price, _last_qty]
    _value = 'value'

    #region #? Prepare for Pivot, optimizing dtype
        # region NOTE  
        # Không được groupby gộp Lot.
        # Không được lọc ngày bắt đầu. 
        # Không được lọc bỏ dòng có end_value == 0
        # Mỗi value point kể cả 0 là 1 anchor để ffill (row value >= 0 MUST KEEP)
        # Lọc mất anchor không thay đổi matrix shape 
        # mà sẽ thay đổi tính toàn vẹn của values sau khi fill
        # endregion NOTE
    pre_pivot = stock_ledger[REQUIRE_COLS].copy()
    pre_pivot[_value] = pre_pivot[_price] * pre_pivot[_last_qty]

    # Cứu RAM
    num_type = 'float64'
    if pre_pivot[_value].max() < 2 ** 31:
        num_type = 'float32'
    pre_pivot[_value] = pre_pivot[_value].astype(num_type)


    pre_pivot  = pre_pivot.set_index(_date)
    start_data = pre_pivot.index.min()
    last_data  = pre_pivot.index.max()
    rein_date  = pd.date_range(start_data, last_data)
    #endregion

    #region #? Pivoting, summing
    # Cân nhắc reindex thêm range vào để tránh gap date trong ledger
    # Giải pháp: SUM xong rồi hãy reindex & ffill

    pivot_result = pd.pivot_table(
        pre_pivot,
        index    =  sku_n_imeisn,
        columns  = _date,
        values   = _value,
        aggfunc  = 'sum',
        observed = True
        ).ffill(axis=1).fillna(0)
    
    #? Thu gọn matrix, gom 'lot' bằng groupby 'sku'
    pivot_compact = pivot_result.groupby(level=s.sku, observed=True).sum()
    #? Collapse matrix và Reindate những ngày gap -> ffill
    inventory_value = pivot_compact.sum().reindex(rein_date).ffill().astype('Int64')

    return inventory_value

@st.cache_data
def get_inventory_as_of(
        ledger_df  : pd.DataFrame,
        AS_OF_DATE : pd.Timestamp,
        AS_OF_BY   : list = [s.sku, s.lot],
        _date      : str  = s.date,
    ):
    """
    ### Trả kết quả tra cứu tồn cuối cho tất cả sản phẩm tại 1 thời điểm (As-Of Date).
        - Bao gồm thông tin sản phẩm và tồn cuối (As-Of Date).
        - Trả kết quả sản phẩm ở level SKU + (Lot Number nếu có)
        - Kết quả bao gồm:
        >>> lookup_result, inventory_value
    - note: lookup_result không bao gồm các cột biến động stock movement

    CRITICAL REQUIREMENT:
    ---------------------
    Cấu hình `AS_OF_BY` phải đạt yêu cầu về tính độc nhất để hàm có thể hoạt động.

    Parameters:
    -----------
    ledger_df : pd.DataFrame
        The stock ledger history containing inventory transactions, balances, and prices.
    AS_OF_DATE : pd.Timestamp
        The target historical date/time to check the inventory status.
    AS_OF_BY : list, default ['sku', 'lot']
        The columns defining the unique inventory entities (e.g., product, batch, location).
    _date : str, default 'date'
        The name of the date column in the original `ledger_df`.
    _value : str, default 'end'
        The name of the column representing the ending inventory quantity.

    """

    _ledger_date: str = f'ledger_{_date}'
    LEDGER     = ledger_df.rename(columns={_date: _ledger_date}).sort_values(by=_ledger_date, ascending=True)
    AS_OF_DATE = pd.Timestamp(AS_OF_DATE).as_unit('ns')

    # valid_cols: (Giữ cột end, bỏ cột số khác)
    numeric_cols = LEDGER.select_dtypes('number').columns
    valid_cols   = [col for col in LEDGER.columns if col not in numeric_cols] + [s.end]

    # Groupby purpose: extract unique product "sku" + according "lot_number" (if any)
    lookup_queue = LEDGER[AS_OF_BY].groupby(AS_OF_BY, as_index=False).head(1).copy()
    lookup_queue[_date] = AS_OF_DATE
    lookup_result = (
        pd.merge_asof(
            left  = lookup_queue,
            right = LEDGER[valid_cols],
            left_on   = _date,
            right_on  = _ledger_date,
            by        =  AS_OF_BY,
            direction = 'backward',
            allow_exact_matches = True
            )
            .dropna(subset=s.end, ignore_index=True)
            .convert_dtypes()
            )

    return lookup_result

@st.cache_data
def get_specific_inventory_as_of(
    ledger_df     : pd.DataFrame,
    start_period  : pd.Timestamp,
    end_period    : pd.Timestamp,
    date_col      : str = s.date,
    first_num_col : str = s.start,
    last_num_col  : str = s.end,
    tree_event    : dict = None
    ):
    """
    ## Tính toán số lượng tồn kho lũy kế (Inventory As-of) động theo cấu trúc event đa tầng.

    - Hàm tự động tạo mask lọc dữ liệu dựa trên Treemap event\
    ,thực hiện reindex từ ngày đầu keyword xuât hiện và tính toán\
    tổng lũy kế (cumsum) cho các biến động kho.

    Note:
    -----
    Do ledger_df chi tiết đến tầng IMEI/SN, việc sum() trực tiếp cột tồn đầu (start) 
    hoặc tồn cuối (end) ở cấp độ SKU/Branch sẽ sai do data bị fragment.
    Giải pháp: Bỏ 2 cột đầu cuối, sum các cột biến động axis = 1 và cumsum.

    Parameters
    ----------
    ledger_df : pd.DataFrame.
    start_period : pd.Timestamp
        Thời điểm bắt đầu lọc dữ liệu đầu ra.
    end_period : pd.Timestamp
        Thời điểm kết thúc lọc dữ liệu và cũng là mốc chặn trên để tính lũy kế.
    date_col : str, default 'date'
    first_num_col : str, default 'start'
        Cột mốc bắt đầu của cụm cột numeric (ví dụ: 'start').
    last_num_col : str, default 'end'
        Cột mốc kết thúc của cụm cột numeric (ví dụ: 'end').
    tree_event : dict, default None
        Dictionary đại diện cho trạng thái các tầng đang được chọn trên giao diện.
        Key là tên cột trong DataFrame, Value là giá trị cần lọc (không phân biệt hoa thường).


    Returns
    -------
    pd.DataFrame or None
        DataFrame chứa chuỗi ngày liên tục từ khi item xuất hiện đến `end_period` kèm cột 'cumsum'.
        Trả về None nếu dữ liệu đầu vào không hợp lệ hoặc không tìm thấy kết quả.
    """
    if (not isinstance(ledger_df, pd.DataFrame) or ledger_df.empty 
        or not isinstance(tree_event, dict) or not tree_event
        or not {date_col, first_num_col, last_num_col}.issubset(ledger_df.columns)
        ): 
        return
    
    #region #? 1. Trích xuất event & tên cột numeric & Tạo helper function
    col_list   = list(tree_event.keys())
    # Helper function
    LOOKUP = lambda series, name: series.str.lower() == name.lower()
    # Numeric không bao gồm 'tồn đầu', 'tồn cuối'
    NUMERIC_COL = ledger_df.loc[:, first_num_col:last_num_col].columns[1:-1]
    if len(NUMERIC_COL) == 0:
        return print("[get_specific_inventory_as_of] Không có data Numerics")
    #endregion #? end
    
    #region #? 2. Gom mask
    combine_mask = ledger_df[date_col] <= end_period
    for col_name, item in tree_event.items():
        if col_name in ledger_df.columns:
            if ledger_df[col_name].dtypes.kind == 'O':
                combine_mask &= ledger_df[col_name].pipe(LOOKUP, item)
    if not combine_mask.any():
        return
    #endregion

    #region #? 3. Groupby(date, key_col)
    key_col      = col_list[-1] # = Most detail col
    pre_output   = (
        ledger_df[combine_mask]
        .groupby([date_col, key_col]) # Output gồm date + key + NUMERIC_COL
        [NUMERIC_COL]
        .sum()  # Drop key khỏi index, giữ date index
        .reset_index(level=key_col, drop=False)
        )
    #endregion #? end

    #region #? 4. Xử lý Nốt: reindex date -> cumsum -> date filter
    # Lấy ngày sku xuất hiện (bảo toàn số liệu trước khi cumsum)
    first_appear = pre_output.index[0]
    REINDATE     = pd.date_range(first_appear, end_period)

    # Fill row những ngày không có biến động
    final_output = pre_output.reindex(REINDATE)
    final_output[key_col] = final_output[key_col].ffill()

    # Tính tổng gộp cho tồn kho
    final_output['cumsum'] = final_output[NUMERIC_COL].sum(axis=1).cumsum()

    # Lấy thời điểm từ start_period
    final_output = (
        final_output[final_output.index >= start_period]
        .reset_index(drop=False)
        .rename(columns={'index': date_col})
        .fillna(0)
        )
    #endregion #? end

    return final_output     

@st.cache_data
def get_stockledger_as_of(
    ledger_df     : pd.DataFrame,
    start_period  : pd.Timestamp,
    end_period    : pd.Timestamp,
    _date             : str  = s.date,
    sku_and_lot       : list = [s.sku, s.lot],
    start_end_numeric : list = [s.start, s.end]
    ):
    """
    ## Logic flow:
    ```
    [Source] ──► mask (start_period đến end_period)
        │
        ▼
    [Snapshot] ──► Fill Tồn đầu kỳ vào dòng ĐẦU TIÊN của từng SKU
        │
        ▼
    Tính tổng biến động từng ngày `sum_change`
        │
        ▼
    Chạy dọc bảng .cumsum() tính ra Tồn cuối ngày
        │
        ▼
    Dùng groupby.shift(1) hạ cả mảng tồn cuối ngày 
    fill vào tồn đầu ngày bên dưới cho từng SKU
    ```
    """
    _sku, _lot   = sku_and_lot
    _start, _end = start_end_numeric

    if (not isinstance(ledger_df, pd.DataFrame) or ledger_df.empty 
        or not {_date, _sku, _lot, _start, _end}.issubset(ledger_df.columns)
        ):
        return
    
    #* NUMERIC not include _start & _end
    if len(NUMERIC_COL:= ledger_df.loc[:, _start:_end].columns[1:-1]) == 0:
        return
    
    if (period_mask := ledger_df[_date].between(start_period, end_period, inclusive='both')).any() == False:
        return
    
    ledger_period = ledger_df[period_mask].copy()

    _note = """
    Thử lấy first record của sản phẩm bán được trong period để rút gọn range -> k ăn thua
    Thử mask bằng sku bán đc -> thiếu data tồn
    -> Giải pháp: lấy snapshot cả kho _end tại thời điểm (start_period - 1), lắp vào ledger_period
    """
   
    start_snapshot = get_inventory_as_of(
        ledger_df  = ledger_df, # Dùng ledger_Full
        AS_OF_DATE = start_period - pd.Timedelta(days=1),
        AS_OF_BY   = [_sku, _lot],  # `as_of` combination cho lookup backward
        _date      = _date
    )
    if len(sku_start_qty_map:= start_snapshot[[_sku, _end]].groupby(_sku)[_end].sum()) == 0:
        return

    # Bỏ lot_number khỏi STRING_COL
    STRING_COL = [col for col in ledger_period.select_dtypes(include=('string', 'object')).columns if col != _lot]
    ledger_period[STRING_COL] = ledger_period[STRING_COL].fillna('-').astype('category')

    # Bước này lọc bỏ sản phẩm missing info | Groupby product ở sku level, bỏ lot_number
    pre_output = (
        ledger_period.groupby([_date, *STRING_COL],
            as_index = False,
            observed = True)
            [NUMERIC_COL]
            .sum()
        )

    # Tạo cột _start map từ `sku_start_qty_map`
    start_period_qty = pre_output[_sku].map(sku_start_qty_map)
    start_iloc       = pre_output.columns.tolist().index(NUMERIC_COL[0])
    pre_output.insert(start_iloc, _start, start_period_qty)

    # Nếu không phải first record: _start = np.nan
    pre_output.loc[pre_output[_sku].duplicated(keep='first'), _start] = np.nan

    # Tạo cột 'sum_change': Tổng biến động bao gồm cả _start (if _start notna)
    curr_numcols = pre_output.select_dtypes('number').columns
    pre_output.insert(0, 'sum_change', pre_output[curr_numcols].sum(axis=1))
    # Tạo cột '_end' = (tổng biến dộng + _start).cumsum()
    end_iloc = pre_output.columns.tolist().index(NUMERIC_COL[-1]) + 1
    pre_output.insert(end_iloc, _end, pre_output.groupby(_sku, observed=True)['sum_change'].cumsum())

    # Fill gap cho cột '_start' bằng hạ cột '_end' 1 dòng và đập vào
    start_gap = pre_output[_start].isna()
    shifted_end = pre_output.groupby(_sku, observed=True)[_end].shift(1)
    pre_output.loc[start_gap, _start] = shifted_end[start_gap]


    #region #? [How to] Reindex multi-index with cross merge & pd.MultiIndex
    # first_record  = pre_output.index.get_level_values(level=_date)[0]
    # date_range    = pd.date_range(first_record, end_period).to_series(name=_date).reset_index(drop=True)
    # unique_index  = pre_output.index.droplevel(_date).unique().to_frame(index=False)
    # combine_index = pd.merge(date_range, unique_index, how='cross')

    _lession = """
    - Region này giữ lại để sau này tham khảo
    Đã thử dùng pivot_table bung pre_output thành wide format với index là date nhưng quá ngốn cpu vì pivot func
    -> Chỉ nên dùng matrix khi có 1 chiều index.
    """
    
    # multi_reindex = pd.MultiIndex.from_frame(combine_index)
    # final_ledger  = pre_output.reindex(multi_reindex)
    #endregion
    # final_output = pre_output.reset_index(drop=True).set_index([_date, *STRING_COL]).drop(columns='sum_change')


    final_output = pre_output.reset_index(drop=True).drop(columns='sum_change', errors='ignore')

    return final_output

@st.cache_data
def get_compact_stockledger(
    ledger_source     : pd.DataFrame,
    end_period        : pd.Timestamp,
    _date             : str  = s.date,
    sku_and_lot       : list = [s.sku, s.lot],
    start_end_numeric : list = [s.start, s.end]
    ):
    """
    ## Logic flow:
    ```
    [Source Ledger] ──► Mask (<= end_period) -> Chặt tương lai
        │
        ▼
    .fillna('-').astype('category') -> Sạch NaN để groupby(observed=True)
        │
        ▼
    Phân 3 nhóm cột số + 1 minor_movements (các cột biến động linh tinh)
        │
        ▼
    Chia biến động linh tinh làm 2 phần âm dương -> bơm vào total_import/export
        │
        ▼
    .groupby(STRING_COL, observed=True).sum() -> Nén ledger thành 1 dòng/SKU
        │
        ▼
    Tạo cột `ngày bán cuối` và `ngày nhập cuối` + last 60d sales
        │
        ▼
    Ép khoảng cách từ ngày tra cứu tới `ngày .. cuối` = số nguyên
        │
        ▼
    NUMERIC_COL.sum(axis=1) theo chiều ngang -> Chốt tồn cuối kỳ `_end`
    ```
    Args:
    ----------
    ledger_source : pd.DataFrame
    end_period : pd.Timestamp
        today or last data day.
    _date : str, mặc định 'date'
    sku_and_lot : list, mặc định ['sku', 'lot']
        Cặp cột định danh sản phẩm: [_sku, _lot]. Cột _lot sẽ bị gạt bỏ khi gom nhóm.
    start_end_numeric : list, mặc định ['start', 'end']
        Cặp tên cột giới hạn vùng dữ liệu số: [_start, _end]. Các cột biến động nằm giữa cặp này.

    Returns:
    --------
    pd.DataFrame
        Bảng phẳng mỗi SKU đúng 1 dòng chứa tổng Bán, Nhập, Xuất, Tồn cuối và ngày cuối bán + nhập
    """
    _sku, _lot   = sku_and_lot
    _start, _end = start_end_numeric
    #region guard clause
    if (not isinstance(ledger_source, pd.DataFrame)
        or ledger_source.empty 
        or not {_date, _sku, _lot, _start, _end}.issubset(ledger_source.columns)
        ):
        return
    
    if (period_mask := ledger_source[_date] <= end_period).any() == False:
        return
    #endregion


    # Remove _start & _end from NUMERIC (cumsum cols has no value when agg)
    NUMERIC_COL = ledger_source.loc[:, _start:_end].columns[1:-1]

    # Remove _lot from STRING (reduce groupby combinations)
    STRING_COL = [
        col for col in ledger_source.select_dtypes(include=('string', 'object')).columns if col != _lot]

    _note = """
    Cần đảm bảo cột 'string' không tồn tại NaN khi .astype('category')
    để có thể dùng (observed=True) không bị warning.
    - Mục đích giữ lại những sku bị thiếu hụt thông tin
    - Khi groupby các dòng có NaN sẽ bị loại bỏ (bất kể type)
    """

    ledger_df = ledger_source.loc[period_mask, [_date, *STRING_COL, *NUMERIC_COL]].copy()
    #region cleaning
    # Dọn ký tự lỗi trong 'product_name'
    if s.prod_name in ledger_df.columns:
            ledger_df[s.prod_name] = (
                ledger_df[s.prod_name]
                .astype(str)
                .str.replace(r'[\xa0\s]+', ' ', regex=True)
                .str.strip()
            )
    # Lọc bỏ DEMO, mọi DEMO đều empty CAT
    ledger_df = ledger_df.dropna(subset=s.cat, axis=0)
    #endregion
    ledger_df[STRING_COL] = ledger_df[STRING_COL].fillna('-').astype('category')

    #? Phân chia nhóm cột: ...
    minor_movements = [s.stock_take, s.noname_1, s.noname_2, s.noname_3, s.noname_4]

    _total_sell   = 'total_sell'
    _total_import = 'total_import'
    _total_export = 'total_export'

    numeric_configs = {
        _total_sell        : [s.sell, s.returns],
        _total_import      : [s.import_po, s.import_do],
        _total_export      : [s.transfer, s.rtv]
    }

    total_minor_movement = ledger_df[minor_movements].sum(axis=1)
    pos_minor, neg_minor = total_minor_movement.clip(lower=0), total_minor_movement.clip(upper=0)

    for key, cols in numeric_configs.items():
        if key in ledger_df.columns:
            print('Found duplicated col')
            continue
        ledger_df[key] = ledger_df[cols].sum(axis=1)
        if key == _total_import:
            ledger_df[key] += pos_minor
        if key == _total_export:
            ledger_df[key] += neg_minor

    ledger_df = ledger_df.drop(columns=NUMERIC_COL, errors='ignore')
    mask_60d  = ledger_df[_date].between(end_period - pd.Timedelta(days=60), end_period)
    last_60d_sales = ledger_df[mask_60d].groupby(_sku, observed=True)[_total_sell].sum()


    NUMERIC_COL    = list(numeric_configs)
    compact_ledger = ledger_df.groupby(STRING_COL, observed=True, as_index=True)[NUMERIC_COL].sum()

    age_config = {
        'since_last_sales'   : [_total_sell  , lambda x: x.max()],
        'since_last_import'  : [_total_import, lambda x: x.max()],
        'since_first_sales'  : [_total_sell  , lambda x: x.min()],
        'since_first_import' : [_total_import, lambda x: x.min()],
    }
    for col, [lookup_col, func] in age_config.items():
        active_movements = ledger_df[ledger_df[lookup_col] != 0]
        last_updates_map = (pd.Timestamp(end_period) - active_movements.groupby(_sku, observed=True)[_date].pipe(func)).dt.days
        compact_ledger.insert(0, col, compact_ledger.index.get_level_values(_sku).map(last_updates_map))

    compact_ledger[_end] = compact_ledger[NUMERIC_COL].sum(axis=1)
    compact_ledger['last_60d_sales'] = compact_ledger.index.get_level_values(_sku).map(last_60d_sales).fillna(0)

    return compact_ledger

@st.cache_data
def category_stock_status(
    compact_ledger  : pd.DataFrame,
    category        : str = None,
    show_low_supply : bool = False
    ):
    """
    ### Chấm điểm và phân loại tình trạng tồn kho (Demand & Supply Scoring Matrix).
    """
    # Score Map
    _demand_score = 'demand_score'
    demand = {
       -1: '-1. Dead for sure', 
        0: '0. Dead or Display', 
        1: '1. Alert', 
        2: '2. Low', 
        3: '3. Medium', 
        4: '4. High'
    }

    _supply_score = 'supply_score'
    supply = {
       -1: '-1. Ignored', 
        0: '0. Overstock', 
        1: '1. High', 
        2: '2. Balanced', 
        3: '3. Low', 
        4: '4. Review'
    }

    #region 1. FILTER EOL & CLEANING
    invalid_sku  = (compact_ledger[s.end] < 0)
    inactive_sku = (compact_ledger[s.end] == 0) & (compact_ledger['last_60d_sales'] == 0)
    EOL_SKU      = (compact_ledger[s.end] == 0) & (compact_ledger['since_last_import'] >= 180)
    active_df    = compact_ledger.loc[~(EOL_SKU | inactive_sku | invalid_sku)].copy()
    
    neg_cols            = ['total_sell', 'total_export', 'last_60d_sales']
    active_df[neg_cols] = active_df[neg_cols].abs()

    ACC_MASK = active_df.index.get_level_values(s.cat).isin(['APPLE ACC', '3RD ACC'])
    #endregion

    #region 2. KHAI BÁO SERIES & CHỈ SỐ CHUNG
    _first_import = active_df['since_first_import']
    _last_import  = active_df['since_last_import']
    _last_sales   = active_df['since_last_sales']
    _last_60d     = active_df['last_60d_sales']
    _import       = active_df['total_import']
    _sell         = active_df['total_sell']
    _end          = active_df[s.end]

    life_velocity  = _sell / _first_import
    l60d_velocity  = _last_60d / _first_import.clip(lower=1, upper=60)
    day_velocity   = (l60d_velocity * 0.7) + (life_velocity * 0.3)
    month_velocity = (day_velocity * 30).round(1)
    is_vel_grows   = (day_velocity > l60d_velocity) & (_last_60d > 0)

    days_of_cover = np.where(
        day_velocity > 0, 
        _end / day_velocity, 
        np.where(_end > 0, 365.0, 0.0)
    )
    days_of_cover = pd.Series(days_of_cover, index=active_df.index).clip(upper=365.0).round(1)

    is_sold    = _sell > 0
    top_20_dev = _sell[is_sold & ~ACC_MASK].quantile(0.8)
    top_20_acc = _sell[is_sold & ACC_MASK].quantile(0.8)

    top_80_dev = _sell[is_sold & ~ACC_MASK].quantile(0.2)
    top_80_acc = _sell[is_sold & ACC_MASK].quantile(0.2)
    #endregion

    #region 3. #? DEMAND SCORING LOGIC
    active_df[_demand_score] = None
    CRITICAL_SKU = (_end > 0) & (_last_import >= 180) & ( (_last_sales.isna()) | (days_of_cover >= 180) )
    
    top_20_avg = (top_20_dev * 0.3) + (top_20_acc * 0.7)
    TOP_ACTIVE_SKU = (
        (_import > 0)
        & ( (_last_60d >= 20) | (_sell >= top_20_avg) )
        & (_last_sales <= 30)
        & ((_sell / _import) >= 0.5)
    )
    
    TOP_DEV = TOP_ACTIVE_SKU & ~ACC_MASK & (month_velocity >= 4)
    TOP_ACC = TOP_ACTIVE_SKU & ACC_MASK & (month_velocity >= 6)


    just_add_more    = ((_last_import <= 45) & ~TOP_ACTIVE_SKU)
    is_new           = ((_first_import <= 90) & (_last_sales <= 30) & ~TOP_ACTIVE_SKU)
    norrmal_80_group = (~ACC_MASK & (_sell >= top_80_dev)) | (ACC_MASK & (_sell >= top_80_acc))
    
    is_normal_demand = ((
        ~(CRITICAL_SKU | TOP_ACTIVE_SKU) 
        & norrmal_80_group
        )
        | is_new 
        | just_add_more
    )

    # Gán score cho các nhóm đặc thù / đầu cực
    active_df.loc[CRITICAL_SKU, _demand_score] = demand[0]
    active_df.loc[(TOP_ACC | TOP_DEV) & ~CRITICAL_SKU, _demand_score]   = demand[4]
    active_df.loc[~(TOP_ACC | TOP_DEV) & TOP_ACTIVE_SKU, _demand_score] = demand[3]

    # Phân vị tự động nhóm Normal-Demand cho từng nhóm hàng
    demand_setups = [
        {
            "name": "Device",
            "mask": is_normal_demand & ~ACC_MASK,
            "hard_floor": None,
            "q": 3,
            "labels_map": {0: demand[1], 1: demand[2], 2: demand[3]}
        },
        {
            "name": "Accessory",
            "mask": is_normal_demand & ACC_MASK,
            "hard_floor": 3.0,
            "q": 2,
            "labels_map": None  
        }
    ]

    for setup in demand_setups:
        group_mask = setup["mask"]
        if len(active_df[group_mask]) == 0:
            continue
            
        # XỬ LÝ RIÊNG CHO NHÓM CÓ NGƯỠNG SÀN (ACCESSORY)
        if setup["hard_floor"] is not None:
            # Nhánh 1: Dưới sàn tuyệt đối (< 3 cái/tháng) -> Chia đều vào Alert và Slow
            under_floor_mask = group_mask & (month_velocity < setup["hard_floor"])
            under_idx = active_df[under_floor_mask].index
            if len(under_idx) > 0:
                under_rank = month_velocity[under_idx].rank(method='first')
                under_bins = pd.qcut(under_rank, q=2, labels=False)
                active_df.loc[under_idx, _demand_score] = under_bins.map({0: demand[1], 1: demand[2]})
            
            # Nhánh 2: Từ sàn trở lên (>= 3 cái/tháng) -> Chia đều vào Slow và Steady
            above_floor_mask = group_mask & (month_velocity >= setup["hard_floor"])
            above_idx = active_df[above_floor_mask].index
            if len(above_idx) > 0:
                above_rank = month_velocity[above_idx].rank(method='first')
                above_bins = pd.qcut(above_rank, q=2, labels=False)
                active_df.loc[above_idx, _demand_score] = above_bins.map({0: demand[2], 1: demand[3]})
                
        # XỬ LÝ (DEVICE)
        else:
            target_idx = active_df[group_mask].index
            if len(target_idx) > 0:
                demand_rank = month_velocity[target_idx].rank(method='first')
                demand_bins = pd.qcut(demand_rank, q=setup["q"], labels=False)
                active_df.loc[target_idx, _demand_score] = demand_bins.map(setup["labels_map"])

    # Quét nốt rác
    show_mercy = is_vel_grows & active_df[_demand_score].isna()
    active_df.loc[show_mercy, _demand_score] = demand[1]

    dead_no_doubt = active_df[_demand_score].isna()
    active_df.loc[dead_no_doubt, _demand_score] = demand[-1]
    #endregion

    #region 4. #? SUPPLY SCORING LOGIC
    active_df[_supply_score] = None

    active_df['month_velocity'] = month_velocity
    active_df['days_of_cover']  = days_of_cover

    # 1. Nhóm Ignored (Ăn theo Demand Dead)
    ignore_supply = active_df[_demand_score].isin([demand[-1], demand[0]])
    active_df.loc[ignore_supply, _supply_score] = supply[-1]

    # 2. Nhóm Review Required
    review_required = active_df[_supply_score].isna() & ((_end == 0) | (days_of_cover <= 15)) & active_df[_demand_score].isin([
        demand[3], demand[4]
    ])
    active_df.loc[review_required, _supply_score] = supply[4]

    # 3. Phân bổ Supply phổ quát theo DOC động cho Device & Accessory
    supply_setups = [
        {
            "name": "Device",
            "mask": active_df[_supply_score].isna() & ~ACC_MASK,
            "bins": [-np.inf, 30, 60, 180, np.inf],
            "labels": [supply[3], supply[2], supply[1], supply[0]]
        },
        {
            "name": "Accessory",
            "mask": active_df[_supply_score].isna() & ACC_MASK, 
            "bins": [-np.inf, 45, 120, 240, np.inf],
            "labels": [supply[3], supply[2], supply[1], supply[0]]
        }
    ]

    for setup in supply_setups:
        target_idx = active_df[setup["mask"]].index
        if len(target_idx) > 0:
            active_df.loc[target_idx, _supply_score] = pd.cut(
                days_of_cover[target_idx], 
                bins=setup["bins"], 
                labels=setup["labels"], 
                include_lowest=True
            ).astype('string')
    #endregion

    #region 5. OUTPUT & FORMATTING
    if active_df[[_demand_score, _supply_score]].isna().any().any():
        st.error("SKU lọt lưới phân loại.")
        return None
    
    drop_col    = ['since_first_import', 'since_first_sales', 'total_export', 'total_sell', 'total_import']
    active_df   = active_df.drop(columns=drop_col, errors='ignore')

    if (remove_product_infos := [col for col in active_df.index.names if col not in [s.cat, s.prod_name, s.sku]]):
        active_df = active_df.droplevel(remove_product_infos)
        
    if category:
        cat_mask = active_df.index.get_level_values(s.cat) == category
        active_df = active_df[cat_mask]

    if show_low_supply:
        low_supply_mask = active_df[_demand_score].isin([demand[3], demand[4]]) & active_df[_supply_score].isin([supply[3], supply[4]])
        active_df = active_df[low_supply_mask]

    #endregion
    
    # DEBUG
    # print('\n\n Start')
    # print(active_df['demand_score'].value_counts().sort_index())
    # print(active_df['supply_score'].value_counts().sort_index())

    # NOTE Phải reset_index ở cuối, nếu reset trước filter thì trích xuất click cell sẽ bị lỗi
    return active_df.reset_index(drop=False)


def get_avail_stock(sales: pd.DataFrame, stock: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp):
    #region     3. Process top sku by month / category indicator by day 
    from columns import colName as c, colFormat as f
    top_sku_month = (
        sales
        .groupby([c.month, c.cat, c.sku], as_index=False)[c.revenue].sum()
        .sort_values([c.month, c.revenue], ascending=[True, False])
        .groupby([c.month, c.cat]).head(20) # 20 is sweet spot
        .sort_values([c.month, c.cat], ignore_index=True)
    )
    top_sku_month = (
        sales
        .groupby([c.month, c.cat, c.sku], as_index=False)[c.qty].sum()
        .sort_values([c.month, c.qty], ascending=[True, False])
        .groupby([c.month, c.cat]).head(7)
        .sort_values([c.month, c.cat], ignore_index=True)
    )
    raw_stock = get_stockledger_as_of(
        ledger_df    = stock,
        start_period = start,
        end_period   = end
        )
    raw_stock.insert(0, c.month, raw_stock[c.date].dt.strftime(f.month))

    select_sku   = raw_stock[s.sku].isin(top_sku_month[c.sku].unique())
    active_sku   = raw_stock[s.start] >= 0
    df_stock     = raw_stock[select_sku & active_sku].reset_index(drop=True)

    stock_pivot  = pd.pivot_table(
        data     = df_stock,
        values   = s.start,
        index    = [c.month, s.date],
        columns  = [s.cat, s.sku],
        observed = True
    )
    remove_sku  = stock_pivot.sum(axis=0) == 0
    stock_pivot = stock_pivot.loc[:, ~remove_sku].ffill(axis=0).fillna(0)

    stock_avail = (stock_pivot > 0).astype('int')
    avail_mask  = pd.DataFrame(False, index=stock_avail.index, columns=stock_avail.columns)

    # Create month-mask for available matrix
    for (month, cat), sub_df in top_sku_month.groupby([c.month, c.cat]):
        sku_list = [sku for sku in sub_df[s.sku].tolist() if (cat, sku) in stock_pivot.columns]
        if sku_list:
            # masking by month and 1 cat each loop
            avail_mask.loc[month, (cat, sku_list)] = True

    # Apply 2D boolean mask onto raw matrix
    masked_matrix    = stock_avail.where(avail_mask)
    # Horizontal groupby columns level 0 (cat) by
    available_matrix = masked_matrix.reset_index(level=c.month, drop=True).T.groupby(level=s.cat, observed=True).mean().T
    available_matrix.columns = available_matrix.columns.astype('str')
    stock_avail_cols = available_matrix.columns.tolist()
    # endregion

