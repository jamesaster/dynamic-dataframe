import numpy as np
import pandas as pd
import time
import re
import unicodedata
import streamlit as st
from src.columns import colName as c
SS = st.session_state
# colNamed


@st.cache_data(show_spinner='Đang chuẩn bị dữ liệu...')
def prepare_visualize(
    df_in   : pd.DataFrame, 
    _date   : str = c.date, 
    _cat    : str = c.cat, 
    _rev    : str = c.revenue
    )-> pd.DataFrame:
    """
    Chuẩn hóa và làm sạch dữ liệu doanh thu để phục vụ visualization.
    
    Quy trình:
    1. Đồng nhất các giá trị thiếu/không xác định về NaN bằng Regex.
    2. Gom nhóm và phân loại lại danh mục sản phẩm (Category).
    3. Điền 'Unknown' cho các cột định dạng chuỗi (bao gồm cả Hash ID).
    4. Loại bỏ dữ liệu lỗi ở các trục chính (Date, Revenue).
    
    Args:
        df_in (pd.DataFrame): DataFrame thô đầu vào.
        _date (str): Tên cột ngày tháng. Mặc định là 'date'.
        _cat (str): Tên cột danh mục. Mặc định là 'cat'.
        _rev (str): Tên cột doanh thu. Mặc định là 'revenue'.
        
    Returns:
        pd.DataFrame: DataFrame đã làm sạch, sẵn sàng để vẽ biểu đồ.
    """
    # 1. chuẩn hóa <NA> trong string cols
    df = df_in.copy().replace(r'(?i).*unknown.*|.*<na>.*', np.nan, regex=True)

    # 2. Dọn dẹp CAT col
    df[_cat] = (
        df[_cat]
        .str
        .replace('ACCESSORIES (APPLE)', 'APPLE ACC')
        .mask(df[_cat].isin(['QOALA', 'BANK FEE']))
        .fillna('3RD ACC')
        )
    
    # 3. Fill 'Unknown' cho các cột string
    unknown_fill = [
    c.sku, c.imei_sn, c.cat, c.subcat,
    c.prod_name, c.cus_id, c.cus_name, c.cus_email]
    fill_list = df[unknown_fill].select_dtypes(include=['string', 'object']).columns
    df[fill_list] = df[fill_list].fillna('Unknown')

    # 4. Xóa các dòng missing revenue
    df = df.dropna(axis=0, ignore_index=True, subset=[_rev])

    # 5. Đảm báo 1 số trục chính không có <NA>, sort by date
    assert df[_date].notna().all(), f'Check {_date}'
    assert df[_rev].notna().all(), f'Check {_rev}'
    df = df.sort_values(by=_date, axis=0, ascending=True, ignore_index=True)

    print(f"Columns still have <NA>: {df.columns[df.isna().any()].values}")
    return df

#region #* Dynamic Filter Pipeline
@st.cache_data
def query_df_date(
    df           : pd.DataFrame, 
    date_option  : any, 
    date_config  : dict, 
    _date        : str = c.date, 
    period_ratio : int=1
    )-> pd.DataFrame:
    """
    ### Hàm filter DataFrame Stage 1/3 - by `Date`
    #### Lọc theo filter + extend_range dựa trên `period_ratio` để so sánh Delta (Double Period).
    
    >>> date_option:    '1 month', '3 month'... (match với date_config)
    >>> date_option:    'This month' -> f_date = mùng 1 tháng hiện tại
    >>> date_option:    {'Custom': {'From': date_from, 'End': date_end}}
    >>> date_config:    Dict cấu hình số tháng lùi (e.g. {'1 month': 1})
    >>> _date:          Tên cột dữ liệu ngày tháng (mặc định: 'date')
    >>> period_ratio:   Hệ số mở rộng dải ngày (mặc định: 1). 
                        Ví dụ: ratio=2 sẽ lấy thêm 1 kỳ đối xứng về phía quá khứ.

    Return df_filtered_result: pd.DataFrame
    
    - Logic vận hành:
    >>> 1. Xác định kỳ gốc (Current Period) từ date_option.
    >>> 2. Tính delta_1period: 
           - Nếu là Month: Khoảng cách thực tế từ today ngược về n tháng.
           - Nếu là Custom: (End - From) + 1 ngày.
    >>> 3. - extend_range: Lùi f_date thêm: (ratio - 1) * delta_1period.
    
    - Metadata đính kèm:
    >>> df.attrs['period_anchor']: pd.Timestamp
        Là mốc thời gian bắt đầu kỳ hiện tại
        Dùng làm ranh giới chặt đôi dữ liệu để tính Delta so sánh giữa 2 kỳ.
    """

    query_basket = []
    local_dict = {}

    # Define Today = ( Today or last data day )
    today = pd.Timestamp.today().normalize()
    if not (df[_date] == today).any():
        today = df[_date].max()

    current_today = today   # NOTE
    min_date = df[_date].min()

    # Check config, nothing special 
    try:
        look_back = date_config.get(date_option, None)
    except:
        look_back = None

    if isinstance(date_option, dict):

        # Trích xuất 'From' & 'End' từ dict['key']
        [key] = list(date_option.keys())
        period_anchor = pd.Timestamp(date_option.get(key).get('From'))
        raw_e = pd.Timestamp(date_option.get(key).get('End'))
        # delta_1_period = ( End - From ) + 1
        delta_1period = (raw_e - period_anchor) + pd.Timedelta(days=1)

        current_today = raw_e # NOTE

    elif look_back is not None:
        # HACK anchor lùi về mùng 1 tháng hiện tại, nếu look_back = 0
        # NOTE thử nghiệm ( + pd.Timedelta(days=1)) nếu look_back != 0
        #? Thử nghiệm Pass
        period_anchor = (
            (today - pd.DateOffset(months=look_back)).replace(day=1) 
            if look_back == 0 
            else (today - pd.DateOffset(months=look_back) + pd.Timedelta(days=1))
        )

        raw_e = today

        # NOTE thử nghiệm + pd.Timedelta(days=1) cho mọi look_back ?
        # ? Thử nghiệm Pass: Phải + 1 vào delta_1period cho mọi case look_back
        delta_1period = (today - period_anchor) + pd.Timedelta(days=1)

    if isinstance(date_option, dict) or look_back is not None:
        # (period_ratio - 1) = total period - raw period
        extend_range = delta_1period * (period_ratio - 1)
        f_date = period_anchor - extend_range 
        e_date = raw_e
    else:
        # All time case
        f_date = period_anchor = min_date
        e_date = today

    # print('-----------------------------------')
    # print(f'from: {f_date.strftime('%d-%m-%Y')}')
    # print(f' end: {e_date.strftime('%d-%m-%Y')}')
    # print(f'period_anchor: {period_anchor.strftime('%d-%m-%Y')}')
    # print(f'prev <  anchor, curr >= anchor')

    local_dict['f_date'] = f_date
    local_dict['e_date'] = e_date

    query_basket.append(f'`{_date}`.between(@f_date, @e_date)')
    query_string = ' and '.join(query_basket)
    
    df = df.query(query_string, local_dict=local_dict)
    # Đính kèm nha
    df.attrs['period_anchor'] = period_anchor
    df.attrs['today'] = current_today
    return df

@st.cache_data
def get_query_options(
    df            : pd.DataFrame, 
    list_col_name : list = None
    )-> dict:
    """
    ### Hàm filter DataFrame Stage 2/3 - Extract Unique Values
    Input:
    >>> df: pd.DataFrame = df_result from query_df_date()
    return dict_options = {'col_name': [unique_value]}
    """
    if not list_col_name: return {}

    legit_col_names = [col for col in list_col_name if col in df.columns]
    
    unique_limit = {
        c.cat    : 20,
        'sa'     : 20,
        c.staff  : 20,
        c.invoice: 30
    }

    dict_options = {
        col: df[col].dropna().unique().tolist() 
            for col in legit_col_names
                if df[col].nunique() < unique_limit.get(col, 20)
    }

    return dict_options

@st.cache_data
def query_df_final(df: pd.DataFrame, dict_options: dict=None)-> pd.DataFrame:
    """
    ### Hàm filter DataFrame Stage 3/3 - Query execution
    Input:
    >>> df: pd.DataFrame = df_result from query_df_date()
    >>> dict_options: dict = {'col_name': [selected_values]} 
    
    return df_final_result: pd.DataFrame
    
    - Lưu ý: dict_options ở đây là các giá trị người dùng ĐÃ CHỌN từ Stage 2.
    """
    query_basket = []     # Giỏ gom lệnh query
    local_dict = {}       # @params map tên biến 

    if not dict_options or not any(dict_options.values()): return df

    for col_name, values in dict_options.items():
        if not values : continue

        op = 'in' if isinstance(values, list) else '=='
        dynamic_var = 'dynamic_' + col_name
        # print(f'DEBUG: dynamic_var: {dynamic_var}')

        local_dict[dynamic_var] = values
        query_basket.append(f'`{col_name}` {op} @{dynamic_var}')

    final_query = ' and '.join(query_basket) 
    # print(f'final_query: {final_query}')
    # print(f'local_dict: {local_dict}')

    return df.query(final_query, local_dict=local_dict)
#endregion

#region #* st.DataFrame Interaction()
def mini_frame_lite(
        data: pd.DataFrame,
        _key_, 
        height=390):
    """
    ## show_dialog Helper
    ## No callback/ No Rerun
    - Show-only st.dataframe()
    - No interaction
    """
    S = "\u2000"
    cc = st.column_config
    st.dataframe(
        data,
        column_config={
            "invoice": cc.TextColumn("Invoice", width=70, alignment="center"),
            "qty"    : cc.NumberColumn("Qty", format="%,d", width=50, alignment="right"),
            "revenue": cc.NumberColumn("Revenue", format="%,d"),
            "staff"  : cc.TextColumn(S + "Staff", alignment="center"),
            "cat"    : cc.TextColumn("Category", width=80, alignment="right"),
            "sku"    : cc.TextColumn(S + "Product ID", alignment="center"),
            "imei_sn": cc.TextColumn(S + "IMEI/Serial", alignment="center"),
            "product_name": cc.TextColumn(S + "Product Name", width=120, alignment="left"),
            "price"  : cc.NumberColumn("Price", format="%,d"),
            "cash"   : cc.NumberColumn("Cash", format="%,d"),
            "card"   : cc.NumberColumn("Credit", format="%,d"),
            "qr_code": cc.NumberColumn("QR-Code", format="%,d"),
            "pct"    : cc.ProgressColumn("Ratio %", format="%.1f%%", min_value=0, max_value=100, width=160),
            "date"   : cc.DateColumn(S + "Date", format="DD-MM-YY", width=80, alignment="center"),
            "time"   : cc.TimeColumn(S + "Time", format="hh:mm A", width=80, alignment="center"),
            "id"     : cc.TextColumn("ID", alignment="left", width=60),
            "name"   : cc.TextColumn("Customer Name", width=140),
            "email"  : cc.TextColumn("Email", width=180)
        },
        height=height,
        placeholder='-',
        hide_index=True,
        key=_key_
    )
# fragment clean, no rerun app
def show_dialog(
        df, 
        clicked_content, 
        cell_history_key: str, 
        table_key: str = 'mini_key', 
        mode: str=c.invoice):
    """
    ## Mode = invoice or cat
    - Show dialog invoice details table.
    - On dismiss table -> auto trigger reset both current + history click keys to prevent UI bug.
    - Bonus: Inject CSS đổi màu `content`

    Args:
        df (pd.DataFrame): Dataframe chứa chi tiết của hóa đơn đã lọc.
        inv_no (str): Mã hóa đơn (Invoice ID) dùng để làm tiêu đề Dialog.
        cell_history_key (str): Key lưu lịch sử click (f'freeze_{table_key}') dùng để theo dõi trạng thái click.
        table_key (str): Key của bảng gốc tạo ra sự kiện click (mặc định là 'mini_key').
    """

    def dimiss_dialog():
        """
        ### Call-back function.
        ### Trigger following keys to default after dismiss dialog table
        """
        SS[cell_history_key] = []
        SS[table_key] = {'selection': {'rows': [], 'columns': [], 'cells': []}}
    @st.dialog(f'Invoice ID: {clicked_content}', width='large', on_dismiss = dimiss_dialog)
    def show_invoice(df):
        """
        ### Displays detailed invoice information and summaries in a Streamlit app.

        - Calculates metrics (items, SKUs, revenue), displays customer and staff 
        - profiles in a Markdown table, and renders a formatted preview dataframe.
        Args:
            df | data (pd.DataFrame): The input dataframe containing invoice records.
        Returns:
            trigger on_dismiss=dimiss_left_mini
        """

        data = df.replace('Unknown', '-')
    
        total_items = data[c.qty].sum()
        unique_skus = data[c.sku].nunique()
        total_revenue = data[c.revenue].sum()
        
        date = data[c.date].dt.strftime('%d-%m-%Y').iloc[0]
        timestamp = data[c.time].dt.time.iloc[0]
        cust_id = ','.join(data[c.cus_id].unique())
        cust_name = ','.join(data[c.cus_name].unique())
        cust_email = ','.join(data[c.cus_email].unique())
        unique_staff = ", ".join(data[c.staff].unique())

        st.markdown(f"""
            | | |
            | ---: | :--- |
            | **`Timestamp:`** | {date} • {timestamp} |
            | **`Sales Staff:`** | {unique_staff} |
            | **`Overview:`** | {total_items} items ({unique_skus} SKUs) |
            | **`Revenue:`** | **{total_revenue:,.0f}** |
            |  |  |
            | **`Customer Profile`** | |
            | **`* ID:`** | {cust_id} |
            | **`* Name:`** | {cust_name} |
            | **`* Email:`** | {cust_email} |
            """)

        ignore_cols = [c.date, c.staff, c.cus_id, c.cus_name, c.cus_email, c.time]
        final = data.drop(columns=ignore_cols, errors='ignore')

        mini_frame_lite(final, 'mini_invoice', height=500)
    @st.dialog(f'Category Name: {clicked_content}', width='large', on_dismiss=dimiss_dialog)
    def show_cat(df):
        """
        ### Displays detailed category information and summaries in a Streamlit app.

        - Calculates category metrics (total items, unique SKUs, total revenue) 
        - Generates a Top 10 Best-Selling Products Markdown summary table with contribution share.
        - Renders a formatted preview dataframe containing specific product details.
        Args:
            df (pd.DataFrame): The input dataframe containing category records.
        """

        data = df.replace('Unknown', '-')
        
        date = data[c.date].dt.strftime('%d-%m-%Y').iloc[0]
        total_items = data[c.qty].sum()
        unique_skus = data[c.sku].nunique()
        total_revenue = data[c.revenue].sum()

        if not data.empty and total_items > 0:
            top_performer = data.groupby(c.prod_name)[c.qty].sum().idxmax()
            top_performer_qty = data.groupby(c.prod_name)[c.qty].sum().max()
            star_product_info = f"**{top_performer}** ({top_performer_qty:,} units)"
        else:
            star_product_info = "-"

        top_10_df = (
            data.groupby(c.prod_name)
            .agg({c.qty: 'sum', c.revenue: 'sum'})
            .sort_values(by=c.qty, ascending=False)
            .head(10)
            .reset_index()
        )
        
        top_10_rows = ""
        for idx, row in top_10_df.iterrows():
            rev_pct = (row[c.revenue] / total_revenue * 100) if total_items > 0 else 0
            top_10_rows += f"| `{idx+1}` | {row[c.prod_name]} | {row[c.qty]:,} | {row[c.revenue]:,.0f} | {rev_pct:.0f}% |\n"
#! CẤM THỤT LỀ
        st.markdown(f"""
| | |
| :--- | :--- |
| **`Date:`** | {date} |
| **`Overview:`** | **{total_items} items** ({unique_skus} SKUs) |
| **`Revenue:`** | **{total_revenue:,.0f}** |
| **`Top seller:`** | {star_product_info} |
""")
        
        pop_up = st.popover('See details', key='Popo_cat', width='stretch', type='secondary', icon='🔍')
        st.caption("***Quick summary of top-10 performing items.***")

        st.markdown(f"""
| **`No.`** | **`Product Name`** | **`Total QTY`** | **`Total Revenue`** | **`% Cont.`** |
| :--- | :--- | :---: | :---: | :---: |
{top_10_rows}""")

        final = data[[c.sku, c.imei_sn, c.prod_name, c.qty, c.revenue]]
        with pop_up:
            mini_frame_lite(final, 'mini_cat', height=500)
        st.space('large')
    
    if mode == c.invoice:
        show_invoice(df)
    elif mode == c.cat:
        show_cat(df)
    else:   return
@st.cache_data
def get_mini_data(
    df: pd.DataFrame, 
    event: dict,
    agg_target: str
    ):
    """
    ## Hàm này prepare df input cho st.dataframe
    - Aggregate theo 1 cột target và lấy chính xác 1 ngày từ Hero chart event
    - GROUPBY 2 ALWAYS AGG 1 (>1 = Thảm họa)
    - GROUPBY 1 AGG bao nhiêu cũng đc
    """
    _col = c.date
    if pd.notna(event) and any(df[_col] == event):
        _val = event
    else:
        return pd.DataFrame(np.full((10, 4), ''), columns=['Please', 'check', 'event', 'Date'])

    mask = df[_col] == _val
    data_frame: pd.DataFrame = df.loc[mask]

    agg_target = [agg_target] #? Phải trong list
    agg_config = {c.qty: 'sum', c.revenue: 'sum'}

    agg_sort = c.revenue
    agg = (data_frame
            .groupby(agg_target)
            .agg(agg_config)
            .sort_values(agg_sort, ascending=False)
            .reset_index()
            )
    
    total_revenue = agg[agg_sort].sum()
    agg['pct'] = ((agg[agg_sort] / total_revenue))*100

    cal_col_total = lambda c: agg[c].sum(axis=0)

    total = pd.DataFrame([
        {agg_target[0]: f'🔢 Total: {len(agg)}'} | {c: cal_col_total(c) for c, _ in agg_config.items()}
        ]) #? agg_target[0] > cột đầu luôn hiện total
    
    concat = pd.concat([total, agg], ignore_index=True)
    concat['pct'] = concat['pct'].fillna(100)

    format_vnd = lambda col: col.map(
        lambda x: f"{x/1e9:,.1f} B" if abs(x) >= 1e9 
            else f"{x/1e6:,.1f} M" if abs(x) >= 1e6 
            else f"{x/1e3:,.0f} k" if abs(x) >= 1e3 
            else f"{x:,.0f}"
    )
    num_col = concat.select_dtypes(include='number').columns
    concat[num_col] = concat[num_col].apply(format_vnd)
    
    return concat
@st.fragment
def interact_DataFrame(
        df_interaction  : pd.DataFrame,
        df_full_cover   : pd.DataFrame,
        interact_Date   : pd.Timestamp,
        interact_COL    : str,
        table_KEY       : str, # Also clicking_key
        invalid_row     : int = 0,
        height          : int = 360
        ):
    """
    ## Tạo dataframe tương tác (Bảng hiển thị + Bắt tọa độ Click + Kích hoạt Dialog)
    
    Hàm vận hành dưới dạng một `@st.fragment` khép kín nhằm cô lập vòng đời re-render cục bộ. 
    Khi người dùng tương tác chọn ô trên bảng, hệ thống tự động nhận diện tọa độ, lọc dữ liệu 
    và đẩy thẳng lên Dialog chi tiết mà không gây tải lại (rerun) toàn bộ ứng dụng.

    Flow guide:
    ```
    [Render st.dataframe] ──> [on_select='rerun'] ──> [Capture Tọa độ Click]
                                                                           │
    [Bật show_dialog()] <── [Lọc Dữ liệu & Loại các cột phụ = 0 ] <────────┘ (Nếu trúng Target)
        │
        └──> call_back: dimiss_dialog()  ──> clear (current & history) click.
    ```

    Args:
        df_interaction (pd.DataFrame): DataFrame phân mảnh (Mini Frame) hiển thị trực tiếp trên UI.
        df_full_cover (pd.DataFrame): DataFrame tổng quan (Master Dataset) dùng để truy vấn dữ liệu gốc.
        interact_Date (pd.Timestamp): Ngày neo dữ liệu. Nếu truyền `pd.NaT`, hệ thống tự động bốc 
            giá trị ngày từ dòng được click để làm mốc lọc (Khắc phục lỗi index[0]).
        interact_COL (str): Tên cột mục tiêu được phép kích hoạt sự kiện (ví dụ: 'invoice' hoặc 'cat')
            đồng thời là mode hiển thị Dialog ('invoice' or 'cat').
        table_KEY (str): String định danh duy nhất cho bảng trong Session State, đồng thời dùng để 
            khởi tạo cờ hiệu theo dõi lịch sử click (`previous_{table_KEY}`).
        invalid_row (int, optional): Chỉ mục dòng không hợp lệ (ví dụ: dòng tổng số ở index 0). 
            Mặc định là 0 (chặn click dòng đầu tiên).
        height (int, optional): Mặc định 360 (match với 400 echart).

    Session States được quản lý ngầm:
        st.session_state[table_KEY]: Lấy cấu trúc dữ liệu tương tác st.dataframe() (`selection -> cells`).
        st.session_state[f'previous_{table_KEY}']: Lưu tọa độ click của lượt chạy trước đó để chặn 
            hiện tượng lặp `st.toast` vô hạn (Debounce click).
        Dialog dimiss tự động clear keys -> [] tránh hiện Dialog vô tội vạ

    Behavior & UI Optimizations:
        1. **Auto Layout Mode (`tab_mode`):** Tự động chuyển đổi giao diện bảng (Căn lề, ProgressColumn) 
        dựa trên sự xuất hiện của cột kiểm định thiết bị (`imei_sn`).
        2. **Auto Columns Clean-up:** Tự động quét tổng thể hệ thống, drop các cột phương thức thanh toán 
        hoặc số liệu bằng 0 (`cash`, `card`, `qr_code`) nhưng bảo lưu các cột cốt lõi (`qty`, `price`, `revenue`).

    Returns:
        None (Hàm thực hiện render UI và điều hướng State trực tiếp tại Fragment).
    """
    if not isinstance(df_interaction, pd.DataFrame) or df_interaction.empty:
        st.info('No records found.', icon='🐧')
        return
    def blue_col_style(df: pd.DataFrame, cols: list, color='#1F6FEB'):
        cols = [c for c in cols if c in df.columns]
        styling = {'subset': cols, 'font-weight': '500', 'color': color}
        return df.style.set_properties(**styling)
    #region function local config:
    CC = st.column_config
    _space_ = "\u2000"
    tab_mode = False if c.imei_sn in df_interaction.columns else True
    styled_cols = [c.invoice, c.cat]
    #endregion

#region # [interact_DataFrame] #? 1. Show Interaction Table

    st.dataframe(
        data           = blue_col_style(df_interaction, styled_cols),
        column_config  = {
            "invoice": CC.TextColumn("Invoice", width=None if tab_mode else 70, alignment="right" if tab_mode else "center"),
            "qty"    : CC.NumberColumn("Quantity" if tab_mode else "Qty", format="%,d", width=None if tab_mode else 50, alignment="right"),
            "revenue": CC.NumberColumn("Revenue", format="%,d") if not tab_mode else CC.TextColumn('Revenue', alignment="right"),
            "staff"  : CC.TextColumn(_space_+ "Staff", alignment="center"),
            "cat"    : CC.TextColumn("Category", width=80, alignment="right"),
            "sku"    : CC.TextColumn(_space_+ "Product ID", alignment="center"),
            "imei_sn": CC.TextColumn(_space_+ "IMEI/Serial", alignment="center"),
            "product"\
            "_name"  : CC.TextColumn(_space_+ "Product Name", width=120, alignment="left"),
            "price"  : CC.NumberColumn("Price", format="%,d") if not tab_mode else CC.TextColumn('Price', width=100, alignment="right"),
            "cash"   : CC.NumberColumn("Cash", format="%,d"),
            "card"   : CC.NumberColumn("Credit", format="%,d"),
            "qr_code": CC.NumberColumn("QR-Code", format="%,d"),
            "pct"    : CC.ProgressColumn("Ratio %", format="%.1f%%", min_value=0, max_value=100, width=160),
            "date"   : CC.DateColumn(_space_+ "Date", format="DD-MM-YY", width=80, alignment="center"),
            "time"   : CC.TimeColumn(_space_+ "Time", format="hh:mm A", width=80, alignment="center"),
            "id"     : CC.TextColumn("ID", alignment="left", width=60),
            "name"   : CC.TextColumn("Customer Name", width=140),
            "email"  : CC.TextColumn("Email", width=180)
        },
        height         = height,
        placeholder    = '-',
        hide_index     = True,
        key            = table_KEY,
        on_select      = 'rerun',
        selection_mode = 'single-cell'
        
    )
    
#endregion
#region # [interact_DataFrame] #? 2. Click logic & Context Capture
    
    click_loc: list  = SS.get(table_KEY).get('selection', {}).get('cells', [])
    curr_click: tuple = click_loc[0] if click_loc else None
    #? cell_history_key:
    pre_click = f'previous_{table_KEY}'

    if not pre_click in SS or curr_click is None:
        # Khởi tạo key pre_click, -> return
        SS[pre_click] = None
        return
    
    # 'click_loc' RAW format = [(iloc_row, loc_col)]
    click_col, click_row = curr_click[1], curr_click[0]


    valid_row = lambda x: x > (invalid_row if isinstance(invalid_row, int) else -1)

    if  (curr_click != SS[pre_click] and (not valid_row(click_row) or click_col != interact_COL)):
        # Khi click sai điều kiện row & col + cell mới, toast -> return
        SS[pre_click] = curr_click
        st.toast(
            f'Please click on an \
            {interact_COL.title()}\
            ID to view details.', icon="🔍"
            )
        return

    clicked_content = 'Default = Missed'
    if click_col == interact_COL and valid_row(click_row):
        clicked_content = (
            df_interaction.reset_index(drop=True)
            .loc[click_row, click_col]
            )
        #region #* [Debug] 25/05/26:
        #? st.dataframe() trả selection location dạng (iloc: row, loc: col)
        #? Nên reset_index sau khi click và dùng LOC cho lành
        #endregion

#endregion
#region # [interact_DataFrame] #? 3. Masking, Re-touching and Show Dialog 

    REQUIRED_COLS = [
        c.date, c.staff, c.sku, c.imei_sn,
        c.prod_name, c.price, c.qty,
        c.revenue, c.pay_cash, c.pay_card, c.pay_qr,
        c.time, c.cus_id, c.cus_name, c.cus_email
    ]
    _date = c.date

    interact_Date = pd.Timestamp(df_interaction[_date].values[click_row]) if (interact_Date is pd.NaT) else interact_Date

    if clicked_content != 'Default = Missed':
        date_mask = df_full_cover[_date] == interact_Date
        colu_mask = df_full_cover[interact_COL] == clicked_content

        #region # ** DEBUG ** 19-05-26
        # Sự cố khi dùng cùng lookup_table, 
        # `event_date` truyền vào mặc định index[0] để chốt gây False nếu click row > [0]
        # Đã cập nhật logic 'interact_Date'
        #endregion

        interacted_result = df_full_cover.loc[date_mask & colu_mask, REQUIRED_COLS]

        sum_col = interacted_result.sum(axis=0, numeric_only=True)
        keep_num_col = [c.qty, c.price, c.revenue]
        final_drop_col = sum_col[sum_col == 0].drop(keep_num_col, errors='ignore').index

        show_result = interacted_result.drop(columns=final_drop_col)
        show_dialog(
            df = show_result,
            clicked_content = clicked_content, 
            cell_history_key = pre_click,
            table_key = table_KEY,
            mode = interact_COL
            )

#endregion
#endregion

#region #* Finder
def bỏ_dấu(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ""
    
    text = text.replace('Đ', 'D').replace('đ', 'd')
    
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if not unicodedata.combining(c)
    )
# Bỏ @st.fragment
def finder(
    df: pd.DataFrame,
    date_input_default: pd.Timestamp,
    _name: str = c.cus_name, # Để là name
    _date: str = c.date,
    finder_text_input_key: str = 'Searching',
    finder_date_select_key: str = 'Searching_Date'
    ):
    """
    Universal dataframe lookup for invoices, customers, and related records.

    Features
    --------
    - Supports searching by:
        - invoice
        - sku
        - imei_sn
        - customer name
        - email
        - id
        - date

    - Vietnamese accent-insensitive search.
    - Automatic date detection using `dayfirst=True`.
    - Partial keyword matching with regex boundary support.
    - Returns at most 50 matched records.

    Parameters
    ----------
    df : pd.DataFrame
        Source dataframe containing transaction records.

    _name : str, default='name'
        Customer/name column used for normalized text lookup.

    _date : str, default='date'
        Date column used for date matching.

    Returns
    -------
    pd.DataFrame | None, pd.Timestamp
        Filtered dataframe if matches exist, otherwise None.

    Notes
    -----
    - Date column is excluded from string conversion to preserve datetime consistency.
    - Uses normalized Vietnamese text matching via `bỏ_dấu()`.
    - Intended for Finder / universal search workflows.
    """

    #region #? 1. Call_back / reset flag

    if SS['Finder_event_flag'] == 'Synced':
        # Tắt trạng thái Synced ở lần click button sau
        SS['Finder_event_flag'] = False


    def switch_to_date_mode():
        if SS.get(finder_date_select_key):
            SS['Finder_event_flag'] = True
            date_string = SS[finder_date_select_key].strftime('%d/%m/%Y')
            SS[finder_text_input_key] = date_string

    #endregion
    
    #region #? 0. Tạo widget text|date input
    f_text, f_date = st.columns([2, 1])
    raw_input = f_text.text_input(
        label            = '**Quick Lookup**', 
        max_chars        = 30,
        icon             = None, 
        width            = 'stretch',
        label_visibility = 'collapsed',
        key              = finder_text_input_key, 
        help             = 'Tra cứu ID Hóa đơn, Thông tin khách hàng',
        placeholder      = 'Find anything...'
    )


    if not isinstance(df, pd.DataFrame) or df.empty:
        return None, None
    
    if df[_date].dtypes.kind == 'M':
        min_date = df[_date].min()
        max_date = df[_date].max()
    else: 
        min_date = None
        max_date = 'today'


    # Cần đặt NOTE 'date_input' sau khi tạo 'raw_input' NOTE để ghi đè khi fragment rerun.
    f_date.date_input(
        label            = '**Pick a date**',
        value            = date_input_default,
        min_value        = min_date,
        max_value        = max_date,
        key              = finder_date_select_key,
        on_change        = switch_to_date_mode, # Overwrite text_mode if changed.
        format           = "DD/MM/YYYY",
        disabled         = False,
        label_visibility = 'collapsed'
    )
    
    #? Return cold start
    if 'prev_search' not in SS:
        SS.prev_search = None
        return None, None
    
    #? Clear state khi thoát tab Finder (main đã clear cho raw_input)
    if raw_input == '':
        SS.prev_search = '' 
    #endregion

    #region #* GUIDE: return (None, None) when:
        #- if not isinstance(df, pd.DataFrame) or df.empty:
        #- if not raw_input
        #- if 'prev_search' not in SS (new_session)
        #- if not final_mask.any()    (guard)
    #endregion

    #region #? 2. After Fragment Call_back -> Gắn flag để sync ** main_event **
    #? Khá là buồn cười vì main_event được sync bởi hành động onchange của st.tabs bên ngoài

    if SS['Finder_event_flag'] == True:

        SS['Finder_event_flag'] = 'Synced'
        # OFF st.rerun(scope='fragment')
    elif raw_input != SS.prev_search:

        SS.prev_search = raw_input
        # OFF st.rerun(scope='fragment')
    elif not raw_input or raw_input.strip() == '':
        # print('No rerun.') # NOTE
        return None, None
    #endregion

    #region #? 3. Search Engine
    show_cols = [
        c.date, c.time, c.invoice, c.staff,
        c.sku, c.imei_sn, c.prod_name, c.qty,
        c.revenue, c.cus_id, c.cus_name, c.cus_email
        ]

    # Other = without 'date' and 'name'
    lookup_other_cols = [c.invoice, c.sku, c.imei_sn, c.cus_id, c.cus_email]
    show_cols_no_date = [col for col in show_cols if col != _date]
    
    final_input = bỏ_dấu(raw_input) if raw_input != 'Unknown' else 'Méoooo'
    
    pattern = fr"(?i)\b{re.escape(final_input)}"
    SEARCH_LOGIC = lambda s: s.str.contains(pattern, na=False)

    # `show_cols_no_date` vì Astype str cho `date` rồi ép lại sẽ bị hỏng format ngày
    df_lookup = df[show_cols].reset_index(drop=True)
    df_lookup[show_cols_no_date] = df_lookup[show_cols_no_date].astype('string').fillna('')
    
    date_input = pd.to_datetime(final_input, dayfirst=True, errors='coerce')
    
    if pd.notna(date_input):
        date_series = pd.to_datetime(df_lookup[_date], dayfirst=True, errors='coerce')
        lookup_mask = date_series == date_input
        lookup_output = df_lookup.loc[lookup_mask]
    else:
        # Bỏ dấu riêng cột name
        name_mask = df_lookup[_name].map(lambda cell: bỏ_dấu(cell)).pipe(SEARCH_LOGIC)
        else_mask = df_lookup[lookup_other_cols].apply(SEARCH_LOGIC).any(axis=1)
        final_mask = name_mask | else_mask
        if not final_mask.any():
            return None, None
        lookup_output = df_lookup.loc[final_mask, show_cols].head(50)
    
    # st.dataframe selection trả về (iloc,loc) nên không cần sort, 
    # chỉ cần #! reset_index và dùng .loc khi lấy data click
    #endregion
    
    return lookup_output.replace('Unknown', np.nan), date_input    
# Bỏ @st.fragment
def finder_memory(
    df_finder: pd.DataFrame,
    finder_date: pd.Timestamp,
    _Searching: str = 'Searching',
    limit: int = 5):
    """
    ### Lưu lịch sử tìm kiếm bằng st.session_state
    >>> Kết quả finder trả về tuple(df_finder, finder_date)
    - df_finder: pd.DataFrame
    - finder_date: pd.Timestamp
    - Searching: str = Finder text_input_key SS['Searching']
    - limit : Giới hạn số lượng key word lưu trữ
    """
    #region Helper formatting function
    format_vnd = (
        lambda x: f"{x/1e9:,.1f}B" if abs(x) >= 1e9 
            else f"{x/1e6:,.1f}M" if abs(x) >= 1e6 
            else f"{x/1e3:,.0f}k" if abs(x) >= 1e3 
            else f"{x:,.0f}"
        )
    #endregion
    
    print('MEMORY RUN')
    
    df_finder_is_valid: bool = isinstance(df_finder, pd.DataFrame) and not df_finder.empty
    

    #region #? 1. Setting up default keys & Some Toasts
    search_example = ['20850', 'Trần Anh', '19 09 25', '2KEIUBGGLF', 'hieu']
    if 'search_history' not in SS:
        SS['search_history'] = search_example

    if 'use_search_history' not in SS:
        SS['use_search_history'] = None

    elif SS['use_search_history'] == 'Done':
        if df_finder_is_valid:
            st.toast('Found your records!', icon='🐶')
        else:
            if SS[_Searching]:
                st.toast('Found nothing!', icon='🐧')

    current_query = SS.get(_Searching, '').strip()
    #endregion


    #region #? 2. MEMORY append if: > 2 chữ | không lặp | có kết quả
    if len(current_query) > 2:
        
        # Xóa kết quả cũ nếu lặp để đưa lên đầu
        if current_query in SS['search_history']:
            SS['search_history'].remove(current_query)

        # Chỉ add vào recent nếu có kết quá
        if df_finder_is_valid:
            SS['search_history'].append(current_query)

        # Ghi đè nếu quá limit
        if len(SS['search_history']) > limit:
            SS['search_history'] = SS['search_history'][-limit:]
    #endregion

    
    #region #* 5.  Rerun khi marked 'Ready'
    if SS['use_search_history'] == 'Ready':
        SS['use_search_history'] = 'Done'

        # BỎ st.rerun(scope='fragment')
    #endregion


    #region #* 4. Hàm Callback tự trigger khi click history button -> ghi đè text_input của finder
    def on_history_click(selected_word):
        print('MEMORY CALLBACK\nINJECT SEARCHING KEYWORD')

        if SS[_Searching] != selected_word:
            SS[_Searching] = selected_word
            SS['use_search_history'] = 'Ready'
        else:
            SS['use_search_history'] = 'Same'
    #endregion


    #region #! 3. Trình bày Summary | history Button

    L_col, R_col = st.columns([2, 1])
    R_col.caption("Recent searches:")
    
    #* RIGHT side
    if SS['search_history']:
        for key_word in SS['search_history'][::-1]:
            butt_label = f'{key_word}'
            R_col.button(
                label    = butt_label,
                type     = 'secondary',
                # shortcut = 'Space',
                width    = 'stretch',
                key      = f'Saved_{key_word}',
                on_click = on_history_click,
                args     = (key_word, )
            )


    #* LEFT side
    L_col.caption('Summary:' if df_finder_is_valid else 'Tip:')
    top_summary     = L_col.empty()
    date_options    = L_col.empty()
    not_date_ops    = L_col.empty()
    bottom_note     = L_col.empty()
    blue_tip        = L_col.empty()


    if SS['use_search_history'] == 'Same':
        st.balloons()
        SS['use_search_history'] = 'Snowed'
        bottom_note.info('Already up to date!', icon='💤')
        return


    if df_finder_is_valid:

        numeric_vals = df_finder[[c.qty, c.revenue]].apply(pd.to_numeric, errors='coerce').sum(axis=0).values
        total_qty    = numeric_vals[0]
        total_rev    = format_vnd(numeric_vals[1])
        nunique_inv  = df_finder[c.invoice].nunique()
        nunique_id   = df_finder[c.cus_id].nunique()
        nunique_sku  = df_finder[c.sku].nunique()
        nunique_sa   = df_finder[c.staff].nunique()
        if nunique_inv == 1:
            staff_name = ', '.join(df_finder[c.staff].unique())

        is_date = pd.notna(finder_date)
        total_records = len(df_finder)

        if is_date:
            standardized_date = pd.Timestamp(finder_date).strftime('%d/%m/%Y')
            date_options.info(
               f"""
                - **{total_rev}** Revenue
                - **{nunique_inv}** Invoices
                - **{total_qty}** Items Sold
                - **{nunique_sku}** Unique SKUs
                - **{nunique_id}** Unique Customers
                - **{nunique_sa}** Active Staff
                """)
            
        elif not is_date:
            if nunique_id == 1:
                not_date_ops.info(
                    f"""
                    - **{total_rev}** Revenue
                    - **{nunique_inv}** Invoices
                    - **{total_qty}** Items Sold
                    """)
            elif nunique_inv == 1:
                not_date_ops.info(
                    f"""
                    - **{total_rev}** Revenue
                    - **{total_qty}** Items Sold
                    - Handled by **{staff_name}**
                    """)
            else:
                not_date_ops.info(
                    f"""
                    - **{nunique_id}** Customers
                    - **{nunique_inv}** Invoices
                    """)

        
        icon = '\u200b📅' if is_date else '\u200b🔢'
        label = 'Records' if is_date else 'Matched'
        top_prefix = 'Top ' if total_records == 50 else ''
        searching_string = standardized_date if is_date else SS[_Searching]

        if SS[_Searching] and total_records > 0:
            for i in range(0, total_records + 1, max(1, total_records // 5)):
                top_summary.info(f'🔍 **{i}** Records found...')
                time.sleep(0.03)

            # Ghi đè = kết quả cuối
            top_summary.info(
                f"""
                🔹{icon}{top_prefix} **{total_records}** {label} for "**{searching_string}**".  
                """)


    if not df_finder_is_valid:
        if not SS[_Searching]:
            top_summary.info(f'Enter a **keyword** or pick a **date**', icon='💡')
        if SS[_Searching]:
            top_summary.info(f'No records found.', icon='🐧')
    #endregion
#endregion


