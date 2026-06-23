import re
import pandas as pd
import streamlit as st
from visuals import hyper_bar_chart
from visuals.visuals_helper import *
from src.columns import colName as c
SS = st.session_state
st_metric_style = """
        <style>
            /* 1. Label và Value */
            [data-testid="stMetricLabel"] p {
                color: #49618D !important;
                font-weight: 600 !important;
                font-size: 18px !important;
            }
            [data-testid="stMetricValue"] div {
                color: #3366cd !important;
                font-weight: 400 !important;
                font-size: 40px !important;
            }
        </style>
        """

    # 1. Cấu hình hiển thị

# region #? Step 1
def get_metrics_config(
    tf_distrib    : pd.DataFrame,
    atv_distrib        : pd.DataFrame,
    qty_distrib        : pd.DataFrame,
    period        : str,
    period_anchor : pd.Timestamp,
    today         : pd.Timestamp,
    height        : int = 340
    ):
    def get_metrics_delta(
            period        : str,
            period_anchor : pd.Timestamp,
            today         : pd.Timestamp
            ):
        # Tính số ngày thực tế của kỳ hiện tại
        days_diff = (today - period_anchor).days + 1
        
        if period == 'All time':
            return '—'
        if days_diff == 1:
            return 'vs Yesterday'
        # Nếu "Days" (7 Days, 30 Days...)
        if isinstance(period, str) and period.endswith('Days'):
            return f'vs Prior {days_diff}d' 
        # Custom range or "So Far" (This month so far)
        if isinstance(period, dict) or (isinstance(period, str) and 'So Far' in period):
            return f'vs Prior {days_diff}d'
        # Mặc định cho các mốc Month 
        clean_period = re.sub(r"[a-zA-Z\s\.]+", "", str(period)) + "M"
        
        return f'vs Prior {clean_period}'
    delta_description = get_metrics_delta(
        period        = period,
        period_anchor = period_anchor,
        today         = today
        )
    
    return {
        'Total Revenue': {
            'type': 'line',
            'suffix': None,
            'vs': delta_description,
            'popo_label': None,
            'popo_icon': None
        },

        'Total Traffic': {
            'type': None,
            'suffix': None,
            'vs': delta_description,
            'popo_label': 'Weekly View',
            'popo_icon': '🚶',
            'popo_header': 'Weekly Traffic Trend',
            'hyper_config': {
                'x_data': tf_distrib['date'].tolist(),
                'y_data': tf_distrib['% Distribution'].tolist(),
                'height': height,
                'chart_id': 'traffic_disttr',
                'histogram': True,
                'tooltip_metric_unit': ['Weekday', '']
            }
        },

        'Average Ticket Value': {
            'type': None,
            'suffix': None,
            'vs': delta_description,
            'popo_label': 'Ticket Size',
            'popo_icon': '💵',
            'popo_header': 'Ticket Size Distribution',
            'hyper_config': {
                'x_data': atv_distrib['Revenue Pocket'].tolist(),
                'y_data': atv_distrib['% Distribution'].tolist(),
                'height': height,
                'chart_id': 'atv_disttr',
                'histogram': True,
                'tooltip_metric_unit': ['Range', '']
            }
        },

        'Unit Per Transaction': {
            'type': None,
            'suffix': None,
            'vs': delta_description,
            'popo_label': 'Basket Size',
            'popo_icon': '🏷️',
            'popo_header': 'Basket Size Distribution',
            'hyper_config': {
                'x_data': qty_distrib['Quantity Pocket'].tolist(),
                'y_data': qty_distrib['% Distribution'].tolist(),
                'height': height,
                'chart_id': 'qty_disttr',
                'histogram': True,
                'tooltip_metric_unit': ['Qty', 'pcs']
            }
        },
    }
#endregion

# region #? Step 2
@st.cache_data
def get_four_metrics_data(
    period_double   : pd.DataFrame,
    fix_traffic     : pd.DataFrame,
    period_anchor   : pd.Timestamp,
    config_kpi      : dict
    ) -> list:
    """
    ## Updated 14/05/2026
    ### Chỉ bật sparkline/type cho 'Total Revenue' để tối ưu diện tích UI; 
    ### các chỉ số khác chỉ hiện Big Number (ẩn chart).

    Calculate KPIs and deltas for dashboard display.
    
    Args:
        period_double: DataFrame that cover current and previous periods.
        fix_traffic: Reference data for 'Total Traffic' column.
        period_anchor: Timestamp splitting 'Prev' (before) and 'Curr' (after).
        config_kpi: UI & display logic (e.g., {'Total Revenue': {'type': 'bar', 'suffix': '₫'}}).
        col_map: Mapping logical keys to physical columns (e.g., {'revenue': 'rev_column_1'}).
        metrics_col: List of physical columns to read (e.g., ['date', 'revenue']).

    Example:
        >>> config_kpi = {
            'Total Revenue':        {'type': 'bar',  'suffix': None, 'vs': delta_description},
            'Total Traffic':        {'type': 'bar',  'suffix': None, 'vs': delta_description},
            'Average Ticket Value': {'type': 'line', 'suffix': None, 'vs': delta_description}, 
            'Unit Per Transaction': {'type': 'line', 'suffix': None, 'vs': delta_description},
            }
        >>> column_mapping = {
            'date': 'date',
            'revenue': 'revenue',
            'qty': 'qty',
            'invoice': 'invoice',
            'traffic': 'Total Traffic'
            }
        >>> metrics_col  = ['date', 'invoice', 'qty', 'revenue', 'date_traffic']
        return kpis
    """
    
    _traffic = 'Total Traffic'
    metrics_col  = [c.date, c.invoice, c.qty, c.revenue]
    require_cols = [col for col in metrics_col if col in period_double.columns]
    df_metrics   = period_double[require_cols]

    # 1. Cấu hình groupby('date')
    agg_metrics = {
        'Total Revenue'     : (c.revenue, 'sum'),
        'Total Quantity'    : (c.qty, 'sum'),
        'Total Transaction' : (c.invoice, 'nunique')
        # 'Total Traffic'     : Tính nhánh riêng (fix_traffic)
    }

    # 2. Tính toán
    if True:
        # Chia hàng ngang | divide(axis=1) #? ATV và UPT -> Chỉ để vẽ sparkline
        df_kpis = df_metrics.groupby(c.date, as_index=True).agg(**agg_metrics)
        
        # Join fix_traffic vào để đóng băng traffic
        df_kpis = fix_traffic[[_traffic]].join(df_kpis, how='left').fillna(0)

        # NOTE Ẩn data sparkline vì không cần dùng 
        # df_kpis['Average Ticket Value'] = (df_kpis['Total Revenue'] / df_kpis['Total Transaction']).fillna(0)
        # df_kpis['Unit Per Transaction'] = (df_kpis['Total Quantity'] / df_kpis['Total Transaction']).fillna(0)

        # Chém đôi = mid_point | Tách thành 2 df
        mask_prev = df_kpis.index < period_anchor
        mask_curr = df_kpis.index >= period_anchor
        df_prev = df_kpis[mask_prev]
        df_curr = df_kpis[mask_curr]
        # ---------------------------------------------

        # Cộng hàng dọc | sum(axis=0) | #? Aggregated Metric -> Big Number
        # Tính tổng từng cột rồi vào loop chia sau, tránh lỗi Trung Bình của Trung Bình
        curr_rev, curr_tra, curr_qty = df_curr['Total Revenue'].sum(), df_curr['Total Transaction'].sum(), df_curr['Total Quantity'].sum()
        prev_rev, prev_tra, prev_qty = df_prev['Total Revenue'].sum(), df_prev['Total Transaction'].sum(), df_prev['Total Quantity'].sum()

    # 3. Loop 
    kpis = []
    for name, config in config_kpi.items():
        # Xác định giá trị hiện tại và quá khứ dựa trên loại Metric
        if name == 'Average Ticket Value':
            curr_val = (curr_rev / curr_tra) if curr_tra > 0 else 0
            prev_val = (prev_rev / prev_tra) if prev_tra > 0 else 0
        elif name == 'Unit Per Transaction':
            curr_val = (curr_qty / curr_tra) if curr_tra > 0 else 0
            prev_val = (prev_qty / prev_tra) if prev_tra > 0 else 0
        else:
            # Lấy động từ df_curr theo tên đã agg hoặc join
            curr_val = df_curr[name].sum() if name in df_curr.columns else 0
            prev_val = df_prev[name].sum() if name in df_prev.columns else 0

        # Tính Delta %
        if prev_val > 0:
            delta_val = (curr_val / prev_val - 1) * 100
            delta_str = f'{delta_val:+.1f}%'
        else:
            delta_str = ''
        
        kpis.append({
            'label': name,
            'value': format_metric_number(curr_val) + (config['suffix'] if config['suffix'] else ''),
            'delta': delta_str,
            'data' : get_sparkline(df_kpis.loc[mask_curr, name], max_points = 30) if name == 'Total Revenue' else None,
            'type' : config['type'] if name == 'Total Revenue' else None,
            'vs'   : config['vs'],
            'info' : None,
            'popo_label': config['popo_label'] or 'Pls set name',
            'popo_icon': config['popo_icon'] or '🐶',
            'hyper_config': config.get('hyper_config', {})
        })

    return kpis
#endregion

# region #? Step 3
@st.fragment
def four_metrics(kpis: list , cols_scale: list = [1.25, 1.25, 1, 1])-> list:
    """
    ## Hiển thị cụm 4 chỉ số chính (KPIs) và khởi tạo các vùng trống (st.empty).

    Sử dụng `@st.fragment`.
    Hàm này dựng khung st.metric tích hợp biểu đồ cho Revenue, đồng thời sinh ra 
    và dán nhãn các ô st.empty cho 3 chỉ số còn lại để làm menu Breakdown phía ngoài.

    Args:
        kpis (list[dict]): Danh sách 4 dictionary chứa thông tin chỉ số.
            Mỗi dictionary yêu cầu các key sau:
            - label (str): Tên chỉ số (Ví dụ: 'Total Revenue').
            - value (str): Giá trị đã định dạng (Ví dụ: '10.07B').
            - delta (str): Tỷ lệ tăng trưởng (Ví dụ: '-16.7%').
            - data (list/None): Mảng số liệu ngày để vẽ biểu đồ cho Revenue.
            - type (str/None): Loại biểu đồ ('bar' hoặc 'line').
            - vs (str): Mô tả kỳ so sánh (Ví dụ: 'vs Prior 31d').
            - info (str/None): Chuỗi văn bản hiển thị st.info nếu có.
        cols_scale (list, optional): Tỉ lệ chia độ rộng cho 4 cột layout. 
            Mặc định là [1.25, 1.25, 1, 1].

    Returns:
        list[st.delta_generator.DeltaGenerator]: Danh sách chứa 3 ô trống `st.empty()` 
            của 3 chỉ số (Traffic, ATV, UPT) được return ra ngoài để xử lý Breakdown.

    Example:
        >>> kpis = [
        ...     {"label": "Total Revenue", "value": "10.07B", "delta": "-16.7%", "data": [...], "type": "bar", "vs": "vs Prior 31d", "info": None},
        ...     {"label": "Total Traffic", "value": "24.1k", "delta": "-40.3%", "data": None, "type": None, "vs": "vs Prior 31d", "info": None},
        ...     ...
        ... ]
        >>> slots = four_metrics(kpis)
        >>> # Truy cập slots[0], slots[1], slots[2] ở ngoài hàm để vẽ Breakdown
    """
    st.markdown(st_metric_style, unsafe_allow_html=True)

    kpi_cols = st.columns(cols_scale)
    for col, item in zip(kpi_cols, kpis):
        with col:
            st.metric(
                label               = item.get('label', 'N/A'),
                value               = item.get('value', 0),
                delta               = item.get('delta', '-'),
                chart_data          = item.get('data'),
                chart_type          = item.get('type', 'line'),
                delta_description   = str(item.get('vs')),
                delta_color         = 'gray',
                height              = 'stretch',
                border              = True
            )
            if item['label'] != 'Total Revenue':
                with st.popover(
                    label     = f'**{item.get('popo_label')}**', 
                    key       = item.get('label'),          #! * The Keys
                    width     = 'stretch', 
                    type      = 'secondary', 
                    icon      = item.get('popo_icon'),
                    on_change = 'rerun'
                    ):
                    with st.container(
                        height = 350, 
                        border = False, 
                        key=f'ctn_{item.get('popo_label')}'
                        ):
                        if SS.get(item.get('label')):
                            hyper_bar_chart(**item.get('hyper_config'))
# endregion