from streamlit_echarts import st_echarts, JsCode
from src import get_specific_inventory_as_of
from typing import Callable, Literal, Union
from src.columns import colName as c, stockCol as s, colFormat as f
from src.stockledger import get_inventory_value
from .visuals_helper import *
import streamlit as st
import pandas as pd
import numpy as np
import json
import math
# colNamed

#region Pie   🍰
@st.cache_data
def get_pie_data(
    df          : pd.DataFrame,
    event_date  : pd.Timestamp,
    mode        : Literal['is_event', 'from_event'] = 'is_event',
    agg_target  : str = c.staff,
    _date       : str = c.date
    ):

    from .web_ui import custom_sort

    if df.empty:
        return []
    if mode == 'is_event':
        date_mask = df[_date] == event_date
    elif mode == 'from_event':
        date_mask = df[_date] >= event_date

    df_masked = df[date_mask]  
    pie_data = df_masked.groupby(agg_target, as_index=False, observed=True)[c.revenue].sum()
    pie_data.columns = ['name', 'value']
    return pie_data.sort_values('name', key=lambda x: x.astype('str').map(custom_sort)).to_dict(orient='records')

def pie_chart(# Không được @st.fragment
    pie_data    : list, 
    key         : str,
    height      : int = 380):
    """
    ### Vẽ pie chart.
    - Truyền vào 1 `list of dict` từ `pd.DataFrame.to_dict()` dạng `records`
    >>> pie_data = df[['value', 'name']].to_dict(orient='records')
    """

    if not pie_data:
        st.info("Không có dữ liệu cho mục này.")
        return

    total_value = sum(item.get('value', 0) for item in pie_data)
    total = format_number(total_value)

    title = 'Sales'
    legend = label = emphasis = tooltip = True
    center = ["50%", "57%"]
    radius = ["30%", "65%"]
    colors = ["#6081dd", "#62b9b9", "#ffba66", "#7cbbfa", "#ff8894", "#a4b0be"]
    
    if total_value == 0:
        colors = ["#DAECFF", "#D7EAFF", "#D4E8FF", "#D1E6FF", "#CEE4FF", "#CAE3FF"]
        title = 'Standby'
        total = 'Mode'
        legend = label = emphasis = tooltip = False
        radius = ["50%", "65%"]

    options = {
        "color": colors,
        "animation": True,
        "animationEasing": "cubicOut",           # khi thay đổi dữ liệu ('linear', 'bounceOut',...)
        "animationEasingUpdate": "cubicOut",     # khi thay đổi dữ liệu
        "animationDurationUpdate": 600,          # Thời gian hiệu ứng khi thay đổi dữ liệu (ms)

        "backgroundColor": 'transparent',
        "title": {
            "text": title,
            "subtext": total, 
            "left": "center",
            "bottom": "35.5%"
        },
        "tooltip": {
            "show": tooltip,
            "trigger": "item",
            "formatter": JsCode("""
                (params) => {
                    return `
                        <div style="font-size:0.9rem; font-weight:400;">
                            ${params.name}:  
                            <span style="font-size:0.95rem; font-weight:700;">
                                ${params.value.toLocaleString('vi-VN')} VNĐ
                            </span>
                        </div>
                    `
                }
            """)
        },
        "legend": {
            "top": "0%", 
            "left": "center", 
            "show": legend
        },
        "series": [
            {   "animationType": "scale",
                "animationTypeUpdate": "transition",
                "animationDuration": 1000,
                "animationEasing": "cubicOut",
                "type": "pie",
                "name": False,
                "data": pie_data,
                "center": center,
                "radius": radius,
                "avoidLabelOverlap": True,
                # itemStyle, labelLine, smooth đã được khai báo global trong JAMES_THEME()["pie"]
                "label": {"show": label, "position": "outside", "formatter": "{b}"},
                "emphasis": {
                    "label": {
                        "show": emphasis,
                        "color": "#3A3A3A",
                        "fontSize": 22,
                        "fontWeight": "bold",
                        "formatter": "({d}%)"
                    }
                },
            }
        ],
    }
    st_echarts(
        options=options, 
        theme=JAMES_THEME(),
        height=f'{height}px', 
        key=key
    )
#endregion

#region Pro   👽

# @st.cache_data
# def get_line_pro_data(
#     period_double : pd.DataFrame, 
#     fix_traffic   : pd.DataFrame, 
#     period_anchor : pd.Timestamp,
#     _date         : str = c.date, 
#     _rev          : str = c.revenue,
#     _traffic      : str = 'Total Traffic'
#     ):

#     """
#     - df: input `Period_double_df` to take advances of rolling buffer. (`HAS` affect of `Advance filters`)  
#     - traffic: input `Traffic_fixxed_df` which `has NO` affect of `Advance filters` (Preserve Traffic)
#     >>> return result = traffic.Join(df) 
#     """
#     _daily = 'Daily Revenue'
#     _trend_7d = '7D Trend'



#     if not period_anchor:
#         period_anchor = df[_date].min()

#     require_col = [_date, _rev]
#     groupby_rev  = df[require_col].groupby(_date).agg(**{_daily: (_rev, 'sum')})

#     # Ở đây vẫn cần dùng date_min để tính Rolling
#     fix_traffic_idx = fix_traffic.index.get_level_values(_date)
#     REINDATE = pd.date_range(fix_traffic_idx.min(), fix_traffic_idx.max())
#     df_revenue = groupby_rev.reindex(REINDATE).fillna(0).rename_axis(c.date)
#     df_revenue[_7d_trend] = df_revenue.rolling(window='7D').mean()

#     # Join Traffic & ( Chop by anchor )
#     if traffic.index.name != _date:
#         traffic = traffic.set_index(_date)
#     join_data = traffic.join(df_revenue, how='outer').fillna(0).reset_index() #! Nhớ fillna(0)
#     line_data = join_data.loc[join_data[_date] >= period_anchor].copy()
#     line_data[_date] = line_data[_date].dt.strftime('%Y-%m-%d')
#     line_data = line_data.to_dict(orient='list')

#     config = {}
#     config['x_data'] = line_data[_date]
#     config['y_lists'] = [
#         line_data[_daily], 
#         line_data[_7d_trend], 
#         line_data[_traffic]]

#     return config

@st.cache_data
def get_line_pro_data(
    period_double   : pd.DataFrame, 
    fix_traffic     : pd.DataFrame, 
    period_anchor   : pd.Timestamp,
    _date           : str = c.date, 
    _rev            : str = c.revenue,
    _traffic        : str = 'Total Traffic'
    ) -> dict:
    """
    - period_double: DataFrame đầu vào chứa rolling buffer (có ảnh hưởng bởi Advance filters).
    - fix_traffic: DataFrame traffic cố định (KHÔNG BỊ ảnh hưởng bởi Advance filters).
    >>> return: Dictionary cấu hình x_data và y_lists.
    """
    _daily = 'Daily Revenue'
    _trend_7d = '7D Trend'

    if not period_anchor:
        period_anchor = period_double[_date].min()

    df_traffic  = fix_traffic.copy()
    require_col = [_date, _rev]
    groupby_rev = period_double[require_col].groupby(_date).agg(**{_daily: (_rev, 'sum')})


    if _date in df_traffic.columns:
        df_traffic_idx = df_traffic[_date]
    else:
        df_traffic_idx = df_traffic.index.get_level_values(_date)

  
    REINDATE = pd.date_range(df_traffic_idx.min(), df_traffic_idx.max())
    df_revenue = groupby_rev.reindex(REINDATE).fillna(0).rename_axis(_date)
    df_revenue[_trend_7d] = df_revenue[_daily].rolling(window='7D').mean()

    if df_traffic.index.name != _date and _date in df_traffic.columns:
        df_traffic = df_traffic.set_index(_date)

    join_data = df_traffic.join(df_revenue, how='outer').fillna(0).reset_index()
    
    line_data = join_data.loc[join_data[_date] >= period_anchor].copy()
    line_data[_date] = line_data[_date].dt.strftime('%Y-%m-%d')
    line_data = line_data.to_dict(orient='list')

    config = {
        'x_data': line_data[_date],
        'y_lists': [
            line_data[_daily], 
            line_data[_trend_7d], 
            line_data[_traffic]
        ]
    }

    return config

def get_hero_chart_config(
    period_anchor     : pd.Timestamp,
    today             : pd.Timestamp,
    period_double     : pd.DataFrame,
    fix_traffic       : pd.DataFrame,
    stock_ledger      : pd.DataFrame,
    is_hero_inventory : bool = False
    ) -> dict:

    # 1. Xác định điều kiện hiển thị hiển thị tên Legend
    six_months_back = today - pd.DateOffset(months=6)
    show_daily = period_anchor > six_months_back
    show_7d = period_anchor != (six_months_back + pd.Timedelta(days=1))

    daily_condition = 'Daily Revenue' if show_daily else '_Daily Revenue'
    trend_condition = '7D Trend' if show_7d else '_7D Trend'

    # 2. Khởi tạo config
    rev_n_7dtrend = get_line_pro_data(
        df            = period_double, 
        traffic       = fix_traffic[['Total Traffic']], 
        period_anchor = period_anchor
    ) | {
        'legend_names': [daily_condition, trend_condition, 'Traffic'],
        'vlines': [
            {"date": "2024-09-27", "label": "iPhone 16 NPI\n🔥Sep 27, 2024 "},
            {"date": "2025-09-19", "label": "iPhone 17 NPI\n🔥Sep 19, 2025 "}
        ],
        'is_money': [True, True, False],
        'main_index': 1,
    }

    if is_hero_inventory:
        inventory_value = get_inventory_value(stock_ledger)
        
        hyper_data = inventory_value.loc[period_anchor:today]
        
        rev_n_7dtrend['y_lists'][2] = hyper_data.tolist()
        rev_n_7dtrend['legend_names'][2] = 'Inventory Value'
        rev_n_7dtrend['is_money'][2] = True

    return rev_n_7dtrend

#? Updated, inside big fragment
def line_chart_pro(
    x_data       :list,
    y_lists      :list,
    legend_names :list,
    vlines       :list,
    height       :int  = 410,
    main_index   :int  = 1,
    is_money     :list = [True, True, True]
):
    """
    V3: Bỏ hoàn toàn logic sqrt, hỗ trợ Dual Y-Axis.
    >>> x_data      : [list_date] Trục hoành (Ví dụ: ["2024-01-01", ...])
    >>> y_lists     : [[list_v1], [list_v2], [list_traffic]] Dữ liệu thực, vẽ gì nhận nấy.
    >>> legend_names: ["_Daily", "Trend", "Traffic"] 
                    + Thêm "_" ở đầu tên để mặc định ẩn line
                    + Tên chứa "traffic" (không phân biệt hoa thường) sẽ tự bám vào trục Y bên phải.
    - vlines      : [{"date": "2024-09-27", "label": "iPhone 16"}, ...] Điểm đánh dấu trên nóc.
    - is_money    : Hiện tooltip str + VNĐ

    >>> y_types (NEW - optional):
        - ["revenue", "revenue", "traffic"]
        - Dùng để xác định loại dữ liệu cho từng line (thay thế cách detect bằng string)
        - Nếu không truyền → fallback về logic cũ dựa vào legend_names
        - Giúp tách biệt logic khỏi UI label → ổn định hơn khi scale

    >>> main_index (NEW - optional):
        - Index của line chính (line đậm, có area + markLine)
        - Nếu không truyền → mặc định là line cuối (giữ behavior cũ)
        - Tránh phụ thuộc thứ tự dữ liệu (implicit assumption)

    NÂNG CẤP:
    >>> Dual Y-Axis:
        - Tự động tách biệt thang đo cho Revenue (Trái) và Traffic (Phải)
        - Không còn phụ thuộc hoàn toàn vào tên legend

    >>> Performance:
        - Dùng dict lookup thay cho x_data.index() → tránh O(n^2)

    >>> Tooltip Logic:
        - Backend inject y_types sang JS bằng JSON (json.dumps)
        - Tránh lỗi khi dùng str(list) → đảm bảo tương thích JS chuẩn
        - Tooltip không còn phụ thuộc string name để detect traffic
    """

    Hero_ID       = 'Hero Chart'
    Y_COUNT       = len(y_lists)
    X_LEN         = len(x_data)

    x_index_map = { # Tránh O(n^2) khi tìm date index khi vẽ markLine
        v: i for i, v in enumerate(x_data)
        }
    all_series = []

    #region 📆 Date Range relate logics
    s_date = pd.Timestamp(min(x_data))
    e_date = pd.Timestamp(max(x_data))
    half_year = pd.DateOffset(months=6)
    is_six_month = e_date == (s_date + half_year) - pd.Timedelta(days=1)
    is_too_long  = s_date <= (e_date - half_year)

    # 'Show_Inventory' inside sidebar_options()
    is_inventory = st.session_state.get('Show_Inventory', False)
 
    # Nếu tổng series = 0 (Do trigger ẩn cột)    
    is_trend_zero  = sum(y_lists[1]) == 0.0
    is_trafic_zero = sum(y_lists[2]) == 0.0
    #endregion
    #region 🌈 Color logics
    colors = ["#C6E2FF" ,"#779CD9", "#FFA777"] # "#6086dd" "#779CD9"
    clear = 'rgba(255, 255, 255, 0)'
    hide_n_seek = '#D0D0D0'

    if is_trend_zero:
        main_index = 0
        if is_trafic_zero:
            colors = ["#C6E2FF", hide_n_seek, hide_n_seek]
        else:
            colors = ["#6696E6", hide_n_seek, "#FFB891"]

    chart_types = ['bar', 'bar', 'line']
    line_width  = [0, 1, 1.8]
    if is_six_month:
        colors = ["#A4C8EE" ,hide_n_seek, "#FFB891"]
        main_index = 0
    elif is_too_long:
        chart_types = ['bar', 'line', 'bar']
        if not is_inventory:
            line_width = [0, 1.5, 0]
            colors = ["#C6E2FF" ,"#779CD9", "#ACBFD4"]
            
        if is_inventory:
            line_width = [0, 1, 1]
            chart_types = ['bar', 'line', 'line']
            colors = ["#C6E2FF" ,"#779CD9", '#FF9F43']
    #endregion

    for i in range(Y_COUNT):
        is_main    = (i == main_index)
        is_traffic = (i == Y_COUNT - 1)

        series = {
            "name": legend_names[i],
            "type": chart_types[i],
            "data": [[x_data[row], y_lists[i][row]] for row in range(X_LEN)],
            "yAxisIndex": 1 if is_traffic else 0,
            "z": 10 if is_main else (4 if is_six_month else 8) if is_traffic else 6,
            "silent": True if is_traffic else False,    # Tránh line Traffic che event click

            # --- BAR SPECIFIC ---
            "barWidth": ("30%" if is_main else "37%") if not (is_trend_zero or is_trafic_zero) else "55%",
            "barMinWidth": 3.2 if not is_too_long else 1,
            "barMaxWidth": 32,
            "barGap": "12%",                # Khoảng cách 2 cột cùng 1 ngày
            "large": True,                  # Tối ưu performance cho tập data lớn
            "largeThreshold": 1000,

            # Xử lý nhiễu khi data dày
            "sampling": "lttb",
            "universalTransition": True,
            
            # --- STYLE LOGIC ---
            "itemStyle": {
                "color": colors[i % len(colors)],
                "borderRadius": [1, 1, 0, 0] if (is_main or not is_traffic) else [0, 0, 0, 0],
                "opacity": 1 if is_traffic else 0.95,         # Only for BAR
                "borderWidth": 1 if is_main else 0,                 # Line bao quanh nếu cần
                "borderColor": colors[i % len(colors)]
            },
            
            # --- HOVER EFFECTS ---
            "emphasis": {
                "focus": "series",       # Làm nổi bật cả đường/cụm cột đó, mờ các series khác
                "blurScope": "coordinateSystem", 
                "itemStyle": {
                "opacity": 1,
                "borderWidth": 7,
                "borderColor": get_fade_color(colors[i], 0.35),
                "shadowOffsetX": 0,
                "shadowOffsetY": 0,
                "shadowBlur": 0,
                }
            },

            # --- LINE COMPATIBILITY --- (Chỉ có tác dụng khi type="line")
            "smooth": False if is_main else 0.25,
            "showSymbol": False,
            "triggerLineEvent": True,
            # "step": "middle" if is_traffic else False,
            "lineStyle": {
                "width": line_width[i],
                "color": colors[i % len(colors)],
                "type": 'solid' if is_too_long else 
                   [max(3, min(8, 3 + (X_LEN // 30))), 
                    max(2, min(5, 2 + (X_LEN // 30)))]
                    if is_traffic else "solid",
                "opacity": 0.95 # Line only
            },
        }
        # VLINES CONFIG (Chỉ gắn vào 1 LINE để tránh lặp lại)
        if is_main: # Chỉ gắn event lên đường Revenue
            mark_lines = []
            for line in vlines:
                try:
                    # Tìm index để lấy giá trị trên đường Trend
                    idx = x_index_map[line["date"]]
                    y_value = y_lists[i][idx] * 1.025 # Tránh đè Line
                except:
                    y_value = 0 

                mark_lines.append([
                    {
                        # ĐIỂM 1: Gốc nằm ở dưới Line (Chỉ để xác định vị trí bắt đầu)
                        "coord": [line["date"], y_value], 
                        "symbol": "" # Không để gì ở đây cả
                    },
                    {
                        # ĐIỂM 2: Ngọn nằm ở trên Nócccccccccccccccccccccccccccccccccccc
                        "xAxis": line["date"],
                        "y": 60, # 60 là giữa line trần 
                        "label": {
                            "show": True,
                            "formatter": line["label"],
                            "position": "end", # Nhãn ở trên trần
                            "distance": [0, 7]
                        },
                        "symbol": "circle", # NÚT TRÒN BÂY GIỜ SẼ Ở TRÊN TRẦN
                        "symbolSize": 7
                    }
                ])
            # Main Area
            series["areaStyle"] = {
                "color": {
                    "type": 'linear', "x": 0, "y": int(not is_inventory), "x2": 0, "y2": int(is_inventory),
                    "colorStops": [
                        {"offset": 0, "color": clear},
                        {"offset": 1, "color": "#4B7CDD"}
                    ]
                }
            }
            # Vlines
            series["markLine"] = {
                "z": 1,
                "symbol": ["none", "circle"],   # ! Biểu tượng cuối vline
                "symbolSize": 7,                # ! size biểu tượng
                "lineStyle": {
                    "type": "dashed",
                    "color": "#ff9f43",
                    "width": 1,
                    "shadowBlur": 3,
                    "shadowColor": "rgba(255, 159, 67, 1)"
                },
                "label": {
                    "show": True,
                    "position": "end",
                    "color": "#655A56",
                    "fontSize": 9,
                    # "distance": 7,     # Cách xa
                    "fontWeight": "bold", 
                    "fontFamily": "'SF Pro Display', 'Helvetica Neue', 'Inter', 'Segoe UI', Roboto, Arial, sans-serif",
                    # Bỏ nền 
                    # "backgroundColor": "rgba(255, 224, 178, 0.33)",
                    "borderColor": "rgba(255, 152, 0, 0.35)",        
                    "borderWidth": 1,
                    "padding": [5, 5], # Tạo khoảng trống quanh chữ
                    "borderRadius": 25, # Bo góc cái nhãn
                },
                "data": mark_lines
            }
        # Fill Traffic Line
        if is_traffic:
            series["areaStyle"] = {
                "color": {
                    "type": "linear", "x": 0, "y": 1, "x2": 0, "y2": 0,
                    "colorStops": [
                        {"offset": 1, "color": "#FFBF70" 
                           if is_too_long else "#FEEEDB"},
                        {"offset": 0, "color": clear}
                    ]
                }
            }
        all_series.append(series)

    # Legend name == "_name" thì ẩn
    selected_map = {name: not name.startswith('_') for name in legend_names}
    y_label_formatter = JsCode("""
                        function(value) {
                            if (value >= 1000000000) return (value / 1000000000).toFixed(1) + 'B';
                            if (value >= 1000000) return (value / 1000000).toFixed(0) + 'M';
                            return value.toLocaleString();
                        }
                    """)

    options = {
        "animation": True,
        "animationDurationUpdate": 600,
        "animationEasingUpdate": "cubicOut",
        "backgroundColor": 'transparent',
        "tooltip": {
            "trigger": 'axis',
            "backgroundColor": 'rgba(255, 255, 255, 0.85)',
            "formatter": JsCode(f"""
                function(params) {{
                    const isMoneyList = {str(is_money).lower()}; 
                    
                    let dateObj = new Date(params[0].value[0] || params[0].name);
                    if (dateObj.getHours() === 0) {{ dateObj.setHours(7); }}
                    let ddd = dateObj.toLocaleDateString('en-US', {{ weekday: 'short' }});
                    let dd = String(dateObj.getDate()).padStart(2, '0');
                    let mm = String(dateObj.getMonth() + 1).padStart(2, '0');
                    let yyyy = dateObj.getFullYear();
                    let formattedDate = ddd + ': ' + dd + '-' + mm + '-' + yyyy;
                    
                    let res = '<div style="font-weight:bold; margin-bottom:7px;">' + formattedDate + '</div>';
                    
                    params.forEach(item => {{
                        let val = Array.isArray(item.value) ? item.value[1] : item.value;
                        let displayName = item.seriesName.startsWith('_') ? item.seriesName.substring(1) : item.seriesName;
                        
                        // Chỉ dựa vào isMoneyList để quyết định đơn vị
                        let isMoneyForSeries = (item.seriesIndex !== undefined && isMoneyList[item.seriesIndex] === true);
                        let unit = isMoneyForSeries ? ' VNĐ' : '';
                        
                        res += '<div style="display:flex;justify-content:space-between;gap:30px;">' + 
                            '<span>' + item.marker + displayName + '</span>' + 
                            '<span style="font-size:14px; font-weight:600; font-variant-numeric:tabular-nums;\
                                font-family: "JetBrains Mono", "Roboto Mono"; margin-bottom:5px; letter-spacing:1px;">'\
                                + Math.round(val).toLocaleString() + unit + '</span>' + 
                            '</div>';
                    }});
                    return res;
                }}
            """),
        },
        "legend": { 
            "data": legend_names, 
            "selected": selected_map,
            "top": -2, 
            "icon": 'circle',
            "formatter": JsCode("""
                function(name) {
                    return name.startsWith('_') ? name.substring(1) : name;
                }
            """)
        },
        "grid": {
            "left": '10', "right": '10', "bottom": '10', "top": '60', "containLabel": True },
        "xAxis": {
            "type": 'time', # time
            # Bar cần True để không bị lẹm cột, Line cần False để bám sát lề
            "boundaryGap": True if chart_types[1] == "bar" else False,
            "axisLabel": {
                "color": '#999', "fontSize": 12,
                # NOTE: X-axis Label
                "formatter": {
                    "year": "{yyyy}",
                    "month": "{dd} {MMM} {yy}",
                    "day": "{ee}\n{dd}-{M}",
                },
                "hideOverlap": True
            },
            "axisLine": { "lineStyle": { "color": '#eee' } }
        },
        "yAxis": [
            {
                # TRỤC BÊN TRÁI: REVENUE
                "type": 'value',
                "axisLabel": { 
                    "showMinLabel": False,
                    "color": '#999', "fontSize": 12,
                    "formatter": y_label_formatter
                },
                "z": 0,
                "splitLine": { "lineStyle": { "type": [4, 5], "color": "rgba(0,0,0,0.075)" } }
            },
            {
                # TRỤC BÊN PHẢI: TRAFFIC
                "type": 'value',
                "splitLine": { "show": False },
                "axisLabel": {
                    "showMinLabel": False,
                    "color": '#AAA', "fontSize": 11,
                    "formatter": y_label_formatter
                }
            }
        ],
        "dataZoom": [
            # {
            #     "type": "inside",
            #     "xAxisIndex": [0],
            #     "start": 0,
            #     "end": 100
            # }
            # {     # Tắt Slider
            #     "type": 'slider', 
            #     "height": 10,
            #     "bottom": 10,
            #     "realtime": True,
            #     "throttle": 50,
            #     "filterMode": 'empty',
            #     "showDetail": False, 
            #     "handleSize": '270%' 
            # }
        ],
        "series": all_series
    }
    events = {
        "click": "function(params) { return {date: params.value[0]}; }"
        }
    main_event    =   st_echarts(
        options   =   options,
        events    =   events,
        height    =   f"{height+40}px",
        width     =   '100%',
        key       =   Hero_ID,
        )
    
    return main_event


#endregion

#region Hyper 📊
@st.cache_data
def get_revenue_vs_target_data(  # NOTE _Main_()
    df_dynamic  : pd.DataFrame, 
    df_target   : pd.DataFrame, 
    period_mode : str, 
    _join_on    : str, 
    _revenue    : str=c.revenue
    ):
    """
    Tính toán tỷ lệ phần trăm chênh lệch giữa doanh thu thực tế và mục tiêu (Variance %).

    Args:
        df_dynamic: DataFrame chứa dữ liệu thô đã được filter theo dynamic_mask.
        df_target: DataFrame chứa dữ liệu mục tiêu (targets) theo tháng.
        period_mode: Chế độ xem ('date', 'week', 'month_year'), dùng làm trục X và định danh cột target.
        _join_on: Tên cột chung để merge giữa data và target (thường là 'month_year').
        _revenue: Tên cột chứa giá trị doanh thu thực tế.

    Logic Biến:
        - _target: Tự động map thành '[period_mode]_target' hoặc 'month_target' nếu là mode tháng.
        - Groupby logic: Tự động loại bỏ trùng lặp nếu period_mode trùng với _join_on để tránh lỗi ValueError.
    """
                
    # Xác định tên cột target tương ứng
    _target = period_mode + '_target'
    if c.month == period_mode: 
        _target = 'month' + '_target'

    # Pipeline xử lý dữ liệu, tránh trùng lặp grouper
    chart_4_df = (df_dynamic
        .groupby([period_mode, _join_on] if period_mode != _join_on else _join_on, as_index=False)
        .agg({_revenue: 'sum'})
        .merge(df_target[[_join_on, _target]], 
            on=_join_on, 
            how='left')
    )[[period_mode, _revenue, _target]].groupby(period_mode, as_index=False)[[_revenue, _target]].max()
                        # XXX Overlapping Weekly target solved dirty by select max [_revenue, _target]

    total_revenue = chart_4_df[_revenue].sum()
    total_target = chart_4_df[_target].sum()
    avg_markLine = 0 if total_target == 0 else (total_revenue / total_target - 1) * 100


    # Format trục X nếu view là daily
    if period_mode == c.date:
        x_list = chart_4_df[period_mode].dt.strftime('%d %b').tolist()
    else:
        x_list = chart_4_df[period_mode].astype(str).tolist()

    # Tính toán % Variance
    y_list = (((chart_4_df[_revenue] / chart_4_df[_target]) - 1) * 100).round().tolist()

    return x_list, y_list, round(avg_markLine, 1)

@st.fragment
def hyper_bar_chart(  # NOTE _Main_()
    x_data, 
    y_data,
    avg_markLine: float=0.0,
    height: int=450, 
    chart_id: str='Cấm bỏ trống', 
    chart_title=None,
    histogram: bool=False,
    tooltip_metric_unit: list=['default', 'default']
    ):
    """
    ### Vẽ cả Variance Bar Chart & Histogram Bar Chart
    >>> Variance mode = Default
    >>> histogram = True (Histogram Bar Chart)
    """
    if not x_data:
        st.info('Không đủ dữ liệu, vui lòng chuyển mode hiển thị')
        return

    formatted_data = []
    colors = ["#eb7575", "#dfa26f", "#b4c26a", "#a3c569", "#92c768", "#8bc967", "#83c969", "#7fcc70"]
    colors_2 = ["#0284C7", "#027AF4", "#218CF6", "#3F9EF7", "#5EB0F9", "#7CC2FA", "#9BD4FC", "#BAE6FD"]

    x_max_idx = len(x_data) - 1
    c_max_idx = len(colors) - 1
    
    for i, val in enumerate(y_data):
        color_idx = (
            int(
                (i / x_max_idx) * c_max_idx)
            if x_max_idx > 0 
            else c_max_idx)
        
        if histogram:
            if isinstance(x_data[i], str):
                color_idx = - color_idx - 1
                colors = colors_2

            # Guard limit 
            color = colors[min(c_max_idx, color_idx)]
            radius = [2, 5, 0, 0]
        else:
            if val >= 0:
                color = colors[-1]
                radius = [2, 5, 0, 0]
            else:
                color = colors[0]
                radius = [0, 0, 2, 5]
            
        formatted_data.append({
            "value": val,
            "itemStyle": {"color": color, "borderRadius": radius}
        })

    v_min = min(y_data)
    options = {
        "title": {
            "text": chart_title,
            "left": "center",
            "textStyle": {"fontSize": 18, "fontWeight": "bold"}
        },
        "series": [{
            "type": "bar",
            "barWidth": "75%",
            "data": formatted_data,
            "markLine": None if histogram else {
                "silent": False,
                "symbol": "none",
                "label": {
                    "position": "end",
                    "formatter": f"avg.{'+' if avg_markLine >= 0 else '-'}{avg_markLine}%",
                },
                "lineStyle": {
                    "type": "dashed",
                    "color": "rgba(52, 67, 109, 0.4)",
                    "opacity": 1
                },
                "data": [{"type": "average", "name": "Mean"}]
            }
        }],
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "formatter": JsCode(f"""
                function(params) {{
                    var val = params[0].value;
                    var name = params[0].name;
                    var tooltip_metric = {json.dumps(tooltip_metric_unit)}; 
                    var metric = tooltip_metric[0];
                    var unit = tooltip_metric[1];
                    var isDist = {"true" if histogram else "false"};
                    
                    if (isDist) {{
                        return '<div style="font-weight:bold; font-size:13px; color:#666;">' + metric + ': ' + name + ' ' + unit + '</div>' +
                               '<div style="color:#77cc66; font-size:14px;">Proportion: <b>' + val + '%</b></div>';
                    }} else {{
                        var color = val >= 0 ? '#54bc6a' : '#ec7063';
                        var status = val >= 0 ? '▲ Ahead' : '▼ Behind';
                        var displayVal = val > 0 ? '+' + val : val;

                        return '<div style="font-weight:bold; font-size:13px; color:#666;">' + name + '</div>' +
                               '<div style="color:' + color + '; font-size:14px;">' + status + ': <b>' + displayVal + '%</b></div>';
                    }}
                }}
            """)
        },
        "grid": {
            "top": "10%", 
            "bottom": "0%", 
            "left": "0%", 
            "right": "5%" if histogram else "15%", 
            "containLabel": False 
        },
        "xAxis": {
            "z": 10,
            "type": "category",
            "data": x_data,
            "axisLabel": {
                "interval": "auto",
                "rotate": 0,
                "hideOverlap": True,
            },
            "axisTick": {"show": True},
            "axisLine": {
                "show": True,
                "onZero": True,
                "lineStyle": {
                    "type": "solid", 
                    "color": "rgba(52, 67, 109, 0.7)", 
                    "width": 1.5}
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Proportion (%)" if histogram else "Variance (%)", # Tên trục
            "nameLocation": "end",                                      # Đặt tên ở đầu trục Y (phía trên)
            "nameTextStyle": {
                "align": "center", 
                "padding": [0, 7.5, 15, 0],
                "color": "#999"
            },
            "axisLabel": {"formatter": "{value}%"}
        }
    }

    if histogram:
        options["yAxis"]["min"] = 0
    else:
        if len(y_data) == 1 and y_data[0] > 0:
            options["yAxis"]["min"] = int(-y_data[0])
        elif len(y_data) == 1 and y_data[0] <= 0:
            options["yAxis"]["max"] = int(-y_data[0])
        else: 
            options["yAxis"]["min"] = int((v_min - 20) / 10) * 10 

    return st_echarts(options=options, theme=JAMES_THEME(), height=f"{height}px", key=chart_id)
#endregion

#region Tree  🌳

@st.cache_data # NOTE data
def get_stock_movement(df: pd.DataFrame):
    """
    ## GET STOCK MOVEMENT DATA for tree_event_hyper_chart
    ### INPUT:
    - `df`: DataFrame chứa lịch sử kho (yêu cầu có cột `date`, `cumsum`, và các cột số lượng).
    ### OUTPUT:
    - `dict`: Chứa cấu trúc ECharts chuẩn (`colors`, `all_series`, `legends`, `x_data`).
    """

    if not isinstance(df, pd.DataFrame) or df.empty:
        return {
            'metrics': {
                'label' : ['Stock Count', 'Received', 'Sales Out', 'Transfer Out'],
                'value' : [0, 0, 0, 0],
                'return': None
            }
        }
    x_data = df[c.date].dt.strftime('%d-%m-%Y').tolist()
    y_configs = [
        {"col" : "import_do", "name": "Dispatch",  "color": "#73AEDE", "stack": "Positive"},
        {"col" : "import_po", "name": "Purchase",  "color": "#336EE4", "stack": "Positive"},
        {"col" : "sell",      "name": "Sales",     "color": "#97aabb", "stack": "Negative"},
        {"col" : "transfer",  "name": "Transfer",  "color": "#FBDE39", "stack": "Negative"},

        {"col" : "stock_take","name": "Adjust",    "color": "#5A6B7C", "stack": "Adjustment"},
        {"col" : "rtv",       "name": "RTV",       "color": "#50E596", "stack": "Negative"},

        {"col" : "return",    "name": "Return",    "color": "#FF5555", "stack": "Positive"},
        {"line": "cumsum",    "name": "Balance",   "color": "#FFAC7E"}
    ]

    y_valids = [s for s in y_configs if (COL:= s.get('col') or s.get('line')) in df.columns 
                                  and df[COL].sum() != 0]
    # y_valids = [{"line": "cumsum",    "name": "Balance",   "color": "#FFAC7E"}]
    #region #? Dynamic Y-axis min & max
    posi_cols = [item.get('col') for item in y_valids if item.get('stack') == "Positive"]
    nega_cols = [item.get('col') for item in y_valids if item.get('stack') == "Negative"]
    #? DEBUG 17/06/26: trường hợp df hợp lệ nhưng mà full 0 -> sum() == 0
    if not y_valids:
        return
  
    [stock_col]  = [item.get('line') for item in y_valids if item.get('line')]

    num_min = df[nega_cols].sum(axis=1).min()
    num_max = df[posi_cols].sum(axis=1).max()
    stock_max = df[stock_col].max()

    ratio = stock_max / num_max if num_max > 0 else 1
    second_y = 1 if ratio > 2.5 else 0
    min_y = max_y = False
    if not second_y:
        # Only trigger when 1 Y-axis exist (2 trục mà ép về cùng range thì vô nghĩa)
        min_y = int((num_min - 5) / 5) * 5
        max_y = ((max(num_max, stock_max) + 5) // 5) * 5
    elif second_y and num_max < 5:
        max_y = 5
    #endregion
    #region #? Calculate metric values
    return_stock = int(df[s.returns].sum()) 
    stock_as_of  = int(df[stock_col].iloc[-1])
    receive      = int(df[[s.import_do, s.import_po]].sum(axis=1).sum())
    sold         = int(df[s.sell].sum() + return_stock)
    transfer     = int(df[s.transfer].sum())
    received_str = f"+{receive}" if receive > 0 else str(receive)
    metric_config = {
        'label': ['Stock Count', 'Received', 'Sales Out', 'Transfer Out'],
        'value': [stock_as_of, received_str, sold, transfer],
        'return': f'Exclude {str(return_stock)} return' if return_stock != 0 else None
    }
    #endregion

    all_series = []
    legends = []
    colors = []

    for item in y_valids:
        legends.append(item["name"])
        colors.append(item["color"])
        if "col" in item:
            all_series.append({
                "name": item["name"],
                "type": "bar",
                "stack": item["stack"],
                "data": df[item["col"]].tolist(),
                "itemStyle": {
                    "borderRadius": [2, 2, 0, 0]
                    if item["stack"] == "Positive"
                    else [0, 0, 2, 2]
                    if item["stack"] == "Negative"
                    else None
                    },
                "barWidth": "35%",
                "barMinWidth": 1,
                "large": True,
                "sampling": "lttb",
                "largeThreshold": 1000,
                "universalTransition": True,
            })
        elif "line" in item:
            all_series.append({
                "data": df[item["line"]].tolist(),
                "name": item["name"],
                "type": "line",
                "yAxisIndex": second_y,
                "showSymbol": True,
                "symbol": "circle",
                "symbolSize": 7,
                "lineStyle": {"width": 2.1, "color": item["color"]},
                "itemStyle": {"color": item["color"], "borderColor": "#FFF", "borderWidth": 1},
                "large": True,
                "sampling": "lttb",
                "largeThreshold": 1000,
                "universalTransition": True,
                "smooth": 0.1,
                "z":0
        })
        
    second_y_axis = {
        "type": "value",
        "splitLine": {"show": False},
        "axisLabel": {"showMinLabel": False, "color": "#FF955C", "fontSize": 12, "fontWeight": 600},
    }
    return {
        "colors": colors,
        "all_series": all_series,
        "legends": legends,
        "x_data": x_data,
        "min_y": min_y,
        "max_y": max_y,
        "second_y": second_y_axis,
        "metrics": metric_config
    }

@st.fragment
def tree_event_hyper_chart(
    chart_data: dict,
    chart_id = 'Tree Event Variance Chart', 
    height: int = 400
    ):
    """
    ## RENDER STOCK MOVEMENT CHART
    
    ### INPUT:
    - `chart_data`: Dict cấu trúc dữ liệu từ hàm `get_stock_movement`.
    - `chart_id`: Khóa định danh duy nhất chống trùng lặp chart.
    - `height`: Chiều cao hiển thị (px).
    
    ### ⚡ BEHAVIOR:
    - Vẽ biểu đồ cột chồng (Stacked Bar) kết hợp đường dòng chảy (Balance Line).
    """
    if len(chart_data) == 1:
        return st.info(
            '- Không có biến động trong range đã chọn.\n\n'
            '- Vui lòng kiểm tra hoặc cập nhật Stock Ledger.')
    min_y = int(chart_data.get("min_y")) or None
    max_y = int(chart_data.get("max_y")) or None
    interval = None
    if max_y:
        interval = (max_y / (max_y // 5)) if (max_y // 5) else 5
        max_powof_five = max_y % 5 == 0
        if max_powof_five and max_y > 5:
            interval = max_y / 5
            min_y    = -interval

    chart_options = {
            "animation": True,
            "animationDuration": 600,
            "animationDurationUpdate": 400,
        "color": chart_data["colors"],
        "series": chart_data["all_series"],
        "tooltip": {
            "show": True,
            "trigger": "axis",
            "transitionDuration": 1,
            "axisPointer": {
                "type": "line", "label": {"show": False}},
            # "position": JsCode("""
            #      function (pos) {
            #          var shiftX = pos[0] - 65
            #          return [shiftX, '-18%'];
            #      }"""),
            "confine": True,
            "backgroundColor": "rgba(255, 255, 255, 0.6)",
            "formatter": JsCode("""
                function (params) {
                    let html = `<div style="font-weight: bold; margin-bottom: 5px;">${params[0].axisValue}</div>`;
                    let hasData = false;
                    params.forEach((item, idx) => {
                        let isLast = idx === params.length - 1;
                        let val = item.value ?? 0;
                        if (val !== 0 || isLast) {
                            hasData = true;
                            html += `<div style="display: flex; justify-content: space-between; gap: 10px; margin-top: 3px;">
                                        <span>${item.marker} ${item.seriesName}:</span>
                                        <span style="font-weight: bold;">${val.toLocaleString()}</span>
                                    </div>`;
                        }
                    });
                    return hasData ? html : html + '<div>Không có dữ liệu</div>';
                }
            """)
        },
        "legend": {
            "data": chart_data["legends"],
            "top": "0%",
            "icon": "circle"
        },
        "grid": {
            "left": "3%",
            "right": "3%",
            "bottom": "0%",
            "top": "10%",
            "containLabel": True
        },
        "xAxis": [
            {
                "type": "category",
                "data": chart_data["x_data"],
                "axisLabel": {
                    "color": '#999', "fontSize": 12,
                },
            }
        ],
        "yAxis": [
            {
                "type": "value",
                "axisLabel": {"showMinLabel": True, "color": '#999', "fontSize": 12, "formatter": "{value}"},
                "splitLine": { "lineStyle": { "type": [4, 5], "color": "rgba(0,0,0,0.075)" } },
                "min": min_y,
                "max": max_y,
                "interval": interval
            },
                chart_data["second_y"]
        ],
        "dataZoom": [
            {"type": "inside", "start": 0, "end": 100}
        ]
    }
    return st_echarts(options=chart_options, key=chart_id, height=f'{height}px')

@st.cache_data # NOTE data
def get_tree_data(
    df          : pd.DataFrame, 
    _rev        : str = c.revenue,
    _qty        : str = c.qty,
    layers      : list = [c.cat, c.subcat, c.prod_name, c.sku]
    )-> list:
    """
    ## 🌲 BUILD HIERARCHICAL TREE DATA for treemap_chart()
    ### INPUT:
    - `df`: DataFrame Regular Period.
    - `layers`: Danh sách phân cấp phân tích (mặc định: Cat -> Sub -> Product == SKU).
    
    ### OUTPUT:
    - `list`: Cấu trúc JSON dạng cây lồng nhau phục vụ riêng cho Treemap Chart.
    """
    # Final color set
    colors = [
        "#7097d7", "#71bbbb", "#fbbb6d",
        "#89c0f7", "#f98994", "#85c085", 
        "#a4b0be"]
    CAT_COLORS = {
        'IPHONE'    : colors[0],
        'APPLE ACC' : colors[1], 
        'IPAD'      : colors[2],
        'WATCH'     : colors[3],
        'MAC'       : colors[4],
        '3RD ACC'   : colors[5], 
    }
    REQUIRE_COLS = layers + [_rev] + [_qty]

    df_temp = df[REQUIRE_COLS]
    df_temp.loc[:, c.subcat] = df_temp.loc[:, c.subcat].str.replace(r'^Ip', 'iP', regex=True)

    #? 1. Groupby tất cả layer, tối giản data cho các bước sau
    df_grouped = df_temp.groupby(layers)[[_rev] + [_qty]].sum().reset_index()

    tree_root = []
    for cat_name, cat_df in df_grouped.groupby(layers[0]):

        tree_branch = []
        for sub_cat, sub_df in cat_df.groupby(layers[1]):
            re_names = {
                layers[2] : 'name',
                _rev      : 'value',
                _qty      : 'quantity',
                c.sku     : c.sku # Added for event data
            }

            tree_leaf = sub_df.rename(columns = re_names)[list(re_names.values())].to_dict(orient='records')
            tree_branch.append({
                'name' : sub_cat,
                'value': float(sub_df[_rev].sum()),
                'quantity': float(sub_df[_qty].sum()),
                'children': tree_leaf
                })
            
        tree_root.append({
            'name': cat_name,
            'value': float(cat_df[_rev].sum()),
            'quantity': float(cat_df[_qty].sum()),
            'children': tree_branch,
            'itemStyle': {'color': CAT_COLORS.get(cat_name, '#A0A0A0')} 
        })

    return tree_root

@st.cache_data # NOTE data
def sqrt_tree_v3(source):
    """
    ## 📐 ROOT-ONLY SQRT DATA SCALING
    ### 🧠 LOGIC:
    - Chỉ bóp căn bậc hai (`math.sqrt`) giá trị tổng của các ngành hàng Gốc (Root).
    - Giữ nguyên giá trị của tầng con để tối ưu hóa tỷ lệ hiển thị trên không gian Treemap.
    """

    root_cat  = ['3RD ACC', 'APPLE ACC', 'IPAD', 'IPHONE', 'MAC', 'WATCH']
    sqrt_data = []
    
    for raw in source:

        sqrt = {
            'name'    : raw['name'],
            'rawValue': raw['value'],
            'value'   : math.sqrt(raw['value']) if raw['name'] in root_cat else raw['value'],
            'quantity': raw['quantity'],
        }

        if c.sku in raw:
            sqrt[c.sku] = raw[c.sku]

        if 'itemStyle' in raw:
            sqrt['itemStyle'] = raw['itemStyle']

        if 'children' in raw:
            sqrt['children'] = sqrt_tree_v3(raw['children'])

        sqrt_data.append(sqrt)

    return sqrt_data


def treemap_chart(
    treemap_data: list,
    chart_id = 'Happy_Tree', 
    height: int = 500
    ):
    """
    Render ECharts Treemap with toggle buttons.

    How the buttons work:
    ```
    User clicks Button
        └──► 1. DEFINE KEYS  ──► Tạo 2 button keys (mode, show) và 2 reset indices
        └──► 2. UPDATE STATE ──► Click button ─► switch reset key ⮧ 
                                                        fragment rerun
                    Update index ◄── Trigger cycle (mod) ◄──┘
                            └──► 3. OPTIONS ──► ECharts injects NEW visibleMin
                            └──► 4. END ──► Button auto resets to False 
                                            └──► Smooth render (NO st.rerun)
    ```
    Note:
    -----------
    - Button Key: 
        - Mode: Toggle giữa Raw data và Scaled data (via SQRT).
        - Show: Cycle qua danh sách [All, Standard, Major] để update `visibleMin`.
    - Logic Update (Cycle):
        - Index được lưu trong SS để duy trì trạng thái sau mỗi lần rerun.
        - Công thức: `new_index = (current_index + 1) % len(switch_show_list)`
    
    - visibleMin: Thuộc tính Tree map giới hạn hiển thị của các block < value px².
    - Button Key (trigger) ──► Index State (store) ──► visibleMin (shared_output)

    Parameters:
    -----------
    treemap_data : list
        Hierarchical data structure containing nodes with 'name', 'value', and optional 'children'.
    chart_id : str, default 'Happy_Tree'
        Unique identifier used as a namespace prefix to isolate Session State keys.
    height : int, default 400
        The height of the rendered chart in pixels.
    """
    SS = st.session_state
    reset_keys = [f'{r}_{chart_id}' for r in ['0', '1']]
    button_keys = [f'{b}_{chart_id}' for b in ['mode', 'show']]
    #region #? Button Relays
    reset_view  = reset_keys[0] # View = raw or sqrt
    reset_show  = reset_keys[1] # Show = filter visible min
    is_FIT_now  = SS.get(reset_view, True)

    if SS.get(button_keys[0]):
        is_FIT_now = SS[reset_view] = not SS.get(reset_view, True)

    switch_show_list = [300, 4_000, 10_000]

    if SS.get(button_keys[1]):  # chưa bấm idx = 1, bấm lần đầu idx -> 2 -> 0...
        SS[reset_show] = (SS.get(reset_show, 1) + 1) % len(switch_show_list)
    
    visibleMin         = switch_show_list[SS.get(reset_show, 1)]

    #endregion

    total_revenue = sum(item["value"] for item in treemap_data if "value" in item)
    tooltip_formatter_js = JsCode(f"""
        function (params) {{
            var val = (params.data && params.data.rawValue) ? params.data.rawValue : params.value;
            if (val === undefined || isNaN(val)) return params.name;
            
            var totalVal = {total_revenue};
            var percentStr = '';
            if (totalVal > 0) {{
                var percent = (val / totalVal) * 100;
                percentStr = ' (' + percent.toFixed(1) + '%)';
            }}
            
            var formatted = val >= 1e9 ? (val / 1e9).toFixed(1) + 'B' :
                            val >= 1e6 ? (val / 1e6).toFixed(1) + 'M' :
                            val >= 1e3 ? (val / 1e3).toFixed(1) + 'K' : val;
                            
            var qty = (params.data && params.data.quantity) ? params.data.quantity : 0;
                            
            var itemColor = params.color || '#0f172a'; 
                            
            return '<div style="font-size:14px; font-weight:500; margin-bottom:6px; color:' + itemColor + ';">' + params.name + '</div>' +
                '<span style="color:#64748b;">Revenue:</span> ' +
                '<b style="color:' + itemColor + '; font-size:15px;">' + formatted + ' VNĐ' + percentStr + '</b><br/>' +
                '<span style="color:#64748b;">Sold:</span> ' +
                '<b style="color:' + itemColor + '; font-size:15px;">' + qty + ' pcs</b>';
        }}
    """)
    label_formatter_js = JsCode(f"""
        function (params) {{
            var val = (params.data && params.data.rawValue) ? params.data.rawValue : params.value;
            if (val === undefined || isNaN(val)) return params.name;
            
            var totalVal = {total_revenue};
            var percentStr = '';
            if (totalVal > 0) {{
                var percent = (val / totalVal) * 100;
                percentStr = ' - ' + percent.toFixed(1) + '%';
            }}
            
            var formatted = val >= 1e9 ? (val / 1e9).toFixed(1) + 'B' :
                            val >= 1e6 ? (val / 1e6).toFixed(1) + 'M' :
                            val >= 1e3 ? (val / 1e3).toFixed(1) + 'K' : val;
                            
            var qty = (params.data && params.data.quantity) ? params.data.quantity : 0;
                            
            return params.name + '\\n{{boldVal|' + formatted + percentStr + ' |}} {{qtyStyle|' + qty + ' pcs}}';
        }}
    """)

    options = {
        "tooltip": {
            "show": True,
            "trigger": "item",
            # "position": ['77%', '-18%'],
            "position": JsCode("""
                function (pos) {
                    var shiftX = pos[0] - 50
                    return [shiftX, '-18%'];
                }
            """),
            "confine": False,
            "transitionDuration": 1,
            "backgroundColor": "rgba(255, 255, 255, 0.99)",
            "borderColor": "#eee",
            "borderWidth": 1,
            "textStyle": {"color": "#3A3A3A", "fontSize": 14, "fontWeight": "500"},
            "formatter": tooltip_formatter_js
        },
        "series": [
            {   
                "name": "🏠",
                "type": "treemap",
                "data": sqrt_tree_v3(treemap_data) if is_FIT_now else treemap_data,
                "sort": "desc",
                "left"  : "0%",
                "right" : "0%",
                "top"   : "3%",
                "bottom": "6%",
                "leafDepth": 2,

                # NOTE Default (Normal = 6_000) 
                "visibleMin": visibleMin,

                "nodeClick": "zoomToNode",
                "roam": False,

                "breadcrumb": {
                    "show": True,
                    "left": "center",
                    "bottom": 3,
                    "height": 24,
                    "itemStyle": {
                        "color": "#C7E0FF",
                        "borderColor": "#C7E0FF",
                        "borderWidth": 1,
                        "borderJoin": "round",
                        "textStyle": {
                            "fontSize": 14,
                            "fontWeight": 400,
                            "color": "#3E4D74",
                            "fontFamily": "'SF Pro Display', 'Helvetica Neue', 'Inter', 'Segoe UI', Roboto, Arial, sans-serif",
                            "overflow": "truncate"
                        }
                    },
                    "emphasis": {                  
                        "itemStyle": {
                            "color": "#C7E0FF",
                            "borderColor": "#C7E0FF",
                            "borderWidth": 6,
                            "borderJoin": "round",
                            "textStyle": {
                                "fontSize": 15,
                                "fontWeight": 500,
                            }
                        }
                    },
                },

                # Animation
                "animation": True,
                "animationDuration": 600,
                "animationEasing": "cubicOut",
                "animationDurationUpdate": 250,
                "animationEasingUpdate": "elasticOut",


                "upperLabel": {
                    "show": True,
                    "height": 29,
                    "color": "#3E4D74",
                    "fontWeight": 700,
                    "fontSize": 12,
                    "formatter": label_formatter_js,
                    "rich": {
                        "boldVal": {
                            "fontWeight": "500",
                            "fontSize": 13.5,
                            "color": "#6D7C88"
                        }
                    }
                },
                
                "label": {
                    "show": True,
                    "icon": "none",
                    "position": "inside",
                    "formatter": label_formatter_js,
                    "textStyle": {
                        "color": "#ffffff",
                        "fontSize": 11.5
                    },
                    # Phải bật rich boldVal vì js formatter có set rồi
                    "rich": {
                        "boldVal": {
                            "fontWeight": "bold",
                            "fontSize": 10,
                            "color": "#FFFFFF",
                            "lineHeight": 24
                        },
                        "qtyStyle": {
                            "fontWeight": "600",
                            "fontSize": 10,
                            "color": "#3E4D74",
                            "lineHeight": 24
                        }
                    }
                },
                "itemStyle": {
                    "borderColor": "transparent",
                    "borderWidth": 1.5,
                    "gapWidth": 1,
                    "borderRadius": 6
                },
                "levels": [
                    {
                        #? TẦNG 1 - Khung (Total Revenue)
                        "upperLabel": {"show": False}, #! Tắt hết level 0
                        "emphasis": {"upperLabel": {"show": False,}},
                        "itemStyle": {"gapWidth": False, "borderWidth": False, "borderColor": "transparent"}
                    },
                    {
                        #?  TẦNG 2 - CAT-BIG
                        "colorAlpha": [0.85, 0.95],
                        "upperLabel": {"show": True, "fontSize": 12},
                        "emphasis": {"upperLabel": {"show": True, "fontSize": 14}},
                        "itemStyle": {"gapWidth": 0, "borderWidth": 0} # Cat dont need, bcs sub-cat already got
                    },
                    {
                        #?  TẦNG 3 - sub-cat
                        "colorAlpha": [0.85, 0.95],
                        
                        "upperLabel": {"show": True, "fontSize": 11, "color": "#3770E2"},
                        "emphasis": {
                            "upperLabel": {"show": True, "fontSize": 12},
                            },
                        'itemStyle': {
                            'gapWidth': 4, # Product to product gap
                            'borderWidth': 4 # not only for sub-cat, also cat affected
                        }
                    },
                    {
                        #?  TẦNG 4 - product
                        "colorAlpha": [0.85, 0.95],
                        "emphasis": {
                            "upperLabel": {"show": False, "fontSize": 10,
                            }
                        },
                        "itemStyle": {'borderWidth': 2} #? product only border
                    }
                ]
            }
        ]
    }
    events = {
        "click": """
        function(params) {
            if (params.treePathInfo) {
                let path = params.treePathInfo.map(node => node.name.split(':')[0].trim());
                // Gán SKU vào path nếu tồn tại
                if (params.data && params.data.sku) {
                    path.push(params.data.sku);
                }
                return { path: path };
            }
        }
        """
    }
    tree_event = st_echarts(options=options, theme=JAMES_THEME(), events=events, key=chart_id, height=f'{height}px')

    #region Buttons
    _ , data , _ , show , _ = st.columns([1, 1, 1, 1, 1])

    data_state  = int(is_FIT_now)
    data_config = {
        0: { # Chế độ RAW (is_FIT_now = False)
            'label': '**RAW ON**',
            'icon' : ':material/view_compact:',
            'type' : 'secondary',
            'help' : '**RAW:** Shows actual size based on real data'
        },
        1: { # Chế độ FIT (is_FIT_now = True)
            'label': '**FIT ON**',
            'icon' : ':material/view_comfy:',
            'type' : 'tertiary',
            'help' : '**FIT:** Scales to improve visibility **(Recommended)**'
        }
    }
    get_data    = data_config[data_state]
    data.button(
        label   = get_data['label'],
        icon    = get_data['icon'],
        type    = get_data['type'],
        help    = get_data['help'],
        width   = 'stretch',
        key     = button_keys[0]
    )

    show_state  = SS.get(reset_show, 1)
    show_config = {
        0: {
            'label': '**All**',
            'icon' : ':material/unfold_more_double:',
            'type' : 'secondary',
            'help' : 'Show **everything** regardless of contribution'
        },
        1: {
            'label': '**Standard**',
            'icon' : ':material/unfold_more:',
            'type' : 'tertiary',
            'help' : 'Show **optimized** view'
        },
        2: {
            'label': '**Major**',
            'icon' : ':material/unfold_less:',
            'type' : 'tertiary',
            'help' : 'Show **major items** only'
        }
    }
    get_show    = show_config[show_state]
    show.button(
        label   = get_show['label'],
        icon    = get_show['icon'],
        type    = get_show['type'],
        help    = get_show['help'],
        width   = 'stretch',
        key     = button_keys[1]
        )
    #endregion
    
    return tree_event


# def treemap_n_stock_movement_V0(
#     treemap_data: list,
#     stock_config: dict,
#     today: pd.Timestamp,
#     tree_title: list,
#     chart_id: str,
#     height: int=500,
#     col_ratio: list = [1, 1],
#     vertical: str = 'top',
#     gap: str = 'small'
#     ):
#     """
#     ## 🤝 TWIN-PANEL INVENTORY DASHBOARD
#     ### 🏗️ LAYOUT ARCHITECTURE:
#     - **Cột Trái:** Treemap phân tích cơ cấu sản phẩm (kèm Event Click bắt vị trí).
#     - **Cột Phải:** Biểu đồ xu hướng kho tương ứng + Hệ thống thẻ chỉ số KPI nhanh (`st.metric`).
#     """
#     SS = st.session_state
#     tree_chart, stock_chart = st.columns(col_ratio, vertical_alignment=vertical, gap=gap)
    
#     with tree_chart:
#         tree_header, tree_blue_tip = st.columns([0.95, 0.05])
#         with tree_header:
#             styled_header(tree_title[0], tree_title[1])
#         with tree_blue_tip:
#             CSS_custom_blue_tips('Click', 'block', 'to focus • 🏠 to return', px=8, down=50)
#         st.space(size='xxsmall')
#         tree_event = treemap_chart(
#             treemap_data = treemap_data,
#             chart_id     = chart_id, 
#             height       = height
#         )
        
#         #region #? Event extraction
#         if 'tree_inject' not in SS:
#             SS.tree_inject = {c.cat: 'IPHONE'}
#         tree_map = SS.tree_inject

#         if tree_event and tree_event.get('chart_event') and tree_event.get('chart_event').get('path') != ['🏠']:
#             path = tree_event['chart_event'].get('path', [])[1:]
#             tree_level = [c.cat, c.subcat, c.prod_name, c.sku]
#             tree_map = {level: name for level, name in zip(tree_level, path)}
#         stock_config['tree_event'] = tree_map
        
#         #endregion
#     with stock_chart:
#         sub_names = list(stock_config['tree_event'].values())
#         sub_key   = sub_names[min(2, len(sub_names) - 1)]
#         styled_header('Stock Movement •', sub_key)
#         st.space(size='xxsmall')

#         # Tạo columns phụ cho metric
#         stock, metric = st.columns([0.81, 0.19], gap=gap)
#         metric_height = int(height * 1.1)

#         with stock:
#             # show_result = False
#             movement_df = get_specific_inventory_as_of(**stock_config)
#             if isinstance(movement_df, pd.DataFrame) and not movement_df.empty:
#                 if 'movt_dataframe' not in SS or not SS.movt_dataframe.equals(movement_df):
#                     SS.movt_dataframe = movement_df
#                     # show_result = True

#             # tree_map_value = sku_result = None
#             # if SS.get('callback_lookup_sku') == 'Done':
#             #     SS.callback_lookup_sku = False
#             #     SS.text_input_SKU_id   = ''
#             #     tree_map_value = list(tree_map.values())[-1]

#             #     if tree_map_value and show_result:
#             #         sku_result  = f'Found result for "{tree_map_value}"'
#             #     else:
#             #         sku_result = f'No result for "{tree_map_value}"'

#             stock_movement = get_stock_movement(SS.movt_dataframe)
#             tree_event_hyper_chart(
#                 chart_data = stock_movement,
#                 chart_id   = f'stock_movement{chart_id}',
#                 height     = height         
#             )

#             #region #? Giao diện tìm kiếm 'sku' (Popover) # Abort !!!
#             _, sku_popover, _ = st.columns([1.2, 1, 1])
#             # def callback_lookup_sku():
#             #     SS.callback_lookup_sku = 'Ready'
#             # popover_key = 'manual_sku_movement'
#             # with sku_popover.popover("**Can't find your SKU ?**", width='stretch', type='tertiary', key=popover_key):
#             #     sku_id = st.text_input(        
#             #         label            = '**SKU id**',
#             #         key              = 'text_input_SKU_id',
#             #         max_chars        = 30,
#             #         icon             = None, 
#             #         width            = 250,
#             #         help             = "Give me any SKU, i'll show your it's history",
#             #         placeholder      = 'Any SKU...',
#             #         on_change        = callback_lookup_sku
#             #         )
#             #     if sku_id:
#             #         SS.tree_inject = {c.cat: sku_id}

#             #     range_opt = ['All Time', 'Custom Range']
#             #     range_mode = st.pills('**Range**', range_opt, width='stretch', default=range_opt[0])
#             #     if range_mode == range_opt[1]:
#             #         from_date = pd.to_datetime(
#             #             st.text_input(        
#             #                 label            = '**From** (type in)', 
#             #                 max_chars        = 30,
#             #                 icon             = None, 
#             #                 width            = 'stretch',
#             #                 help             = 'Input any date with day-first',
#             #                 placeholder      = "Ex: 19 9 25  or  19-09-2025"
#             #             ),
#             #             dayfirst=True, errors='coerce'
#             #             )
#             #         end_date  = pd.to_datetime(
#             #             st.date_input(
#             #                 '**To**', value='today', format='DD-MM-YYYY')
#             #             )
#             #         if pd.isna(from_date):
#             #             from_date = end_date - pd.DateOffset(months=1)
                
#             #     st.info(sku_result) if sku_result else None
#             # NOTE Không thể gián đoạn 1 lượt rerun bằng rerun khác
#             # if SS.get('callback_lookup_sku') == 'Ready':
#             #     SS.callback_lookup_sku = 'Done'
                
#             #     print('Rerun lookup_sku')
#             #     try: # NOTE Vì 1 nguyên nhân chưa rõ hàm chạy vào nhánh rerun khi chưa tương tác gì cả 
#             #          # ?[Debug]: Nguyên nhân do xóa Cache thủ công để test widget khác nên bị xung đột
#             #         st.rerun(scope='fragment')
#             #     except st.errors.StreamlitAPIException:
#             #         pass

#             #endregion

#         with metric.container(border=False, height=metric_height, vertical_alignment='bottom'):
#             metric_config = stock_movement["metrics"]
#             stock_return  = metric_config['return']

#             as_of_date = f"As of {today.strftime('%d-%m-%Y')}"
#             for i in range(len(metric_config.get('label', 0))):
#                 st.metric(
#                     label  = metric_config['label'][i],
#                     value  = metric_config['value'][i],
#                     border = True,
#                     height = 'stretch',
#                     delta_description = as_of_date if i == 0 else stock_return if i == 2 else None
#                     )

#endregion
  
#region Target 📈

@st.cache_data
def get_month_progress(
    month_df    : pd.DataFrame,
    target_df   : pd.DataFrame,
    _month      : str,
    _cat        : str = c.cat,
    _rev        : str = c.revenue
    ):
    """
    ## Get data for `horizon_bar_chart()`
    """
    if not isinstance(month_df, pd.DataFrame) or month_df.empty:
        return None
    cat_config = {
    'TOTAL TARGET': {'ratio': 1.00, 'color': '#a4b0be'},
    'IPHONE':       {'ratio': 0.62, 'color': '#779CD9'},
    'IPAD':         {'ratio': 0.12, 'color': "#fbbb6d"},
    'MAC':          {'ratio': 0.12, 'color': '#F98F99'},
    'WATCH':        {'ratio': 0.04, 'color': '#8FC3F7'},
    'APPLE ACC':    {'ratio': 0.08, 'color': '#78BEBE'},
    '3RD ACC':      {'ratio': 0.02, 'color': '#8BC38B'}
        }
    month_colors = [
        '#779CD9', "#97A6E6", '#74B5B5', '#8BC38B',
        '#A8D38D', '#D1DC7E', '#F1D984', '#FBBB6D',
        '#F98F99', "#BDD1DC", "#B7DFED", "#97CCDF"]


    month_count = month_df[_month].nunique()
    month_targer_col: str = 'month_target'

    if month_count == 1:
        exact_month = month_df[_month].values[0]
        month_target = target_df.loc[target_df[_month] == exact_month, month_targer_col].values[0]

        month_progress = month_df.groupby([_month, _cat], as_index=False, observed=True)[_rev].sum()
        
        new_idx = month_progress.last_valid_index() + 1

        total_row = pd.DataFrame(
            [[None, 'TOTAL TARGET', month_progress[_rev].sum()]], 
            columns=month_progress.columns,
            index=[new_idx]
        )
        month_progress = pd.concat([month_progress, total_row])
        month_progress = month_progress.sort_values(_cat, key=lambda x: x.map(custom_sort), ignore_index=True)

        month_progress['color']  = month_progress[_cat].map(lambda x: cat_config.get(x, {}).get('color', '#FFFFFF'))
        month_progress['target'] = month_target * month_progress[_cat].map(lambda x: cat_config.get(x, {}).get('ratio', 0))

        return month_progress.rename(columns={_cat: 'category'})[['category', _rev, 'target', 'color']].to_dict(orient='records')

    elif month_count >= 2:
        # NOTE: Thứ tự các dòng code quan trọng
        
        month_progress: pd.DataFrame = month_df.groupby(_month, as_index=False)[_rev].sum()
        target_map: pd.Series = target_df.set_index(_month)[month_targer_col]

        month_progress['target'] = month_progress[_month].map(target_map)
        
        dt_month = pd.to_datetime(month_progress[_month]).dt.month
        month_progress['color']  = dt_month.map(lambda row: month_colors[row - 1])

        month_progress[_month]   = pd.to_datetime(month_progress[_month]).dt.strftime('%b. %y').convert_dtypes()

        return month_progress.to_dict(orient='records')

@st.fragment
def horizon_bar_chart(
    chart_config: list,
    height: int=400
    ):
    """
    ## Input data from `get_month_progress()`
    """
    if not chart_config:
        return
    category, revenue, target, color = list(chart_config[0])

    categories = [item[category] for item in chart_config]
    target_color = '#114577'

    pct_list = []
    data_style = []
    for item in chart_config:
        pct         = round((item[revenue] / item[target]) * 100) if item[target] > 0 else 0
        met_target  = pct >= 100
        font_size   = min(19, max(14, (pct / 100) * 14))
        font_weight = 550 if pct > 120 else 500 if pct > 100 else 450 if pct > 75 else 400
        base_color  = item[color]
        end_color   = item[color]
        
        pct_list.append(pct)
        data_style.append({
            "value"     : pct,
            "revenue"   : item[revenue],
            "target"    : item[target],
            "rawColor"  : base_color,
            "label": {
                "show": True,
                "position": "right",
                "formatter": "{c}%",
                "color": target_color if met_target else "#4C5979",
                "fontSize": font_size,
                "fontWeight": font_weight
            },
            "itemStyle": {
                # Bo các góc
                "borderRadius": 9,
                "opacity": 0.95,
                "borderWidth": 1, # Làm blur viền
                "borderColor": "rgba(255, 255, 255, 0.25)",
                "color": {
                    "x": 0, "y": 0, "x2": 1, "y2": 0,
                    "colorStops": [
                        {"offset": 0.1, "color": base_color},
                        {"offset": 1.0, "color": end_color}
                    ]
                }
            },
            "emphasis": {
                "itemStyle": {
                    "opacity": 0.9,
                    "borderWidth": 6,
                    "borderColor": "rgba(255, 255, 255, 0.25)",
                    "color": {
                        "x": 1, "y": 0, "x2": 0.7, "y2": 0,
                        "colorStops": [
                            {"offset": 0.99, "color": base_color},
                            {"offset": 0.01, "color": "rgba(255, 255, 255, 0.75)"},
                        ]
                    }
                }
            }
        })

    options = {
        "tooltip": {
            "trigger": "item",
            "formatter": JsCode("""
                function (params) {
                    if (!params.data) return params.name;
                    
                    var rev = params.data.revenue;
                    var kpi = params.data.target;
                    var gapValue = kpi - rev;
                    
                    var formatUnit = function(val) {
                        var absVal = Math.abs(val);
                        return absVal >= 1e9 ? (absVal / 1e9).toFixed(1) + 'B' :
                            absVal >= 1e6 ? (absVal / 1e6).toFixed(1) + 'M' :
                            absVal >= 1e3 ? (absVal / 1e3).toFixed(1) + 'K' : absVal;
                    };
                    
                    var cfgColor = params.data.rawColor || '#0f172a'; 
                    var gapColor = gapValue > 0 ? '#EB7575' : cfgColor;
                    var gapStr = gapValue <= 0 ? 
                        '(Pass) ' + formatUnit(gapValue) + ' VNĐ' : 
                        formatUnit(gapValue) + ' VNĐ';
                                    
                    return '<div style="font-family: "JetBrains Mono", "Roboto Mono"; padding: 4px;">' +
                        '<div style="font-size:16px; font-weight:700; margin-bottom:8px; color:' + cfgColor + ';">' + params.name + '</div>' +
                        '<div style="font-size:14px; color:#6E7D92; font-weight:500;">Progress: ' + 
                            '<span style="font-weight:600; color:#3E4D74;">' + params.value + '%</span>' + 
                        '</div>' +
                        '<div style="font-size:14px; color:#6E7D92; font-weight:500;">Rev./Asg.: ' + 
                            '<span style="font-weight:600; color:#3E4D74;">' + formatUnit(rev) + ' / ' + formatUnit(kpi) + ' VNĐ</span>' + 
                        '</div>' +
                        '<div style="font-size:14px; color:#6E7D92; font-weight:500;">Gap: ' + 
                            '<span style="font-weight:600; color:' + gapColor + ';">' + gapStr + '</span>' + 
                        '</div>' +
                    '</div>';
                }
            """)
        },
        "series": [
            {
                "type": "bar",
                "data": data_style,
                # "barWidth": "35%",
                "barMaxWidth": 21,
                "showBackground": True,
                "backgroundStyle": {
                    "color": "rgba(180, 180, 180, 0.0)",
                    "borderRadius": 9
                }
            }
        ],
        "grid": {
            "left": "0%",
            "right": "14%",
            "top": "0%",
            "bottom": "0%",
            "containLabel": False
        },
        "xAxis": {
            "type": "value",
            "max": max(100, min(120, max(pct_list))),
            "splitLine": {"lineStyle": {"type": [4, 5], "color": "rgba(0,0,0,0.075)"}},
            "axisLabel": {"formatter": "{value}%", "color": "#999"}
        },
        "yAxis": {
            "type": "category",
            "data": categories,
            "inverse": True, 
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "axisLabel": {"fontSize": 14, "fontWeight": 600, "color": "#55668F"}
        },
    }

    st_echarts(options, height=f"{height + 40}px", key="kpi_h_bar_chart")

#endregion

#region scatter & combo 🫧

def ledger_scatter_data(scored_ledger: pd.DataFrame):
    c_doc       = 'days_of_cover'
    c_velocity  = 'month_velocity'
    c_name      = c.prod_name
    c_end       = s.end
    c_demand    = 'demand_score'
    c_supply    = 'supply_score'

    if scored_ledger.empty:
        return {
            "out_of_stock": [],
            "critical": [],
            "others": [],
            "max_x": 30,
            "max_y": 50
        }
    
    cols = [c_doc, c_velocity, c_name, c_end, c_demand, c_supply]
    df = scored_ledger[cols].dropna().copy()
    
    df['key'] = df[c_demand].str.split('.').str[0]
    
    color_map = {
        "-1": "#E74016", 
        "0": "#EB5E3B", 
        "1": '#F98F99', 
        "2": "#FFB050", 
        "3": "#80C8BB", 
        "4": "#77CC77"
    }
    df['color'] = df['key'].map(color_map).fillna('#808080')
    df['itemStyle'] = [{'color': c} for c in df['color']]
    
    df['symbolSize'] = (8.0 + df[c_end] * 1.2).clip(upper=42)
    
    mask_oos = df[c_doc] <= 0
    df.loc[mask_oos, 'symbolSize'] = 10
    df.loc[mask_oos & (df['key'] == '3'), 'symbolSize'] = 12
    df.loc[mask_oos & (df['key'] == '4'), 'symbolSize'] = 20

    
    df['value'] = df[cols].values.tolist()
    mask_critical = (~mask_oos) & df['key'].isin(['-1', '0'])
    mask_others = ~(mask_oos | mask_critical)

    target_cols = ['value', 'symbolSize', 'itemStyle']


    max_y = (int(df[c_velocity].max()) // 10) * 10 + 10
    max_x = (int(df[c_doc].max()) // 10) * 10 + 20
    if max_x - 20 < 90:
        df.loc[mask_others | mask_oos, 'symbolSize'] = df.loc[mask_others | mask_oos, 'symbolSize'] * 2

    return {
        "out_of_stock": df.loc[mask_oos, target_cols].to_dict(orient='records'),
        "critical": df.loc[mask_critical, target_cols].to_dict(orient='records'),
        "others": df.loc[mask_others, target_cols].to_dict(orient='records'),
        "max_x": max_x,
        "max_y": max_y,
    }

@st.fragment
def ledger_scatter_chart(chart_data: dict):
    max_x = chart_data['max_x']
    max_y = chart_data['max_y']
    y_interval = (((max_y // 5) // 10) * 10) + (10 if max_y > 10 else 2)
    stock_ledger_js = """
            function (params) {
                if (!params.value) return '';
                
                let doc       = params.value[0];
                let velocity  = params.value[1];
                let prodName  = params.value[2];
                let stockQty  = params.value[3];
                let demandStr = params.value[4];
                let supplyStr = params.value[5];
                let itemColor = params.color;
                
                let mos       = + (doc / 30).toFixed(1); 
                let cleanText = (s) => {
                    if (!s) return '';
                    let idx = s.indexOf('.');
                    return idx !== -1 ? s.substring(idx + 1).trim() : s;
                };
                
                return `
                    <div style="font-weight:600; border-bottom:1px solid #ccc; margin-bottom:5px; color:${itemColor};">${prodName}</div>
                    
                    <div style="display:flex; justify-content:space-between; margin-bottom:2px; gap:20px;">
                        <span>Demand:</span> <span style="font-weight: 500;">${cleanText(demandStr)}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:2px; gap:20px;">
                        <span>Supply:</span> <span style="font-weight: 500;">${cleanText(supplyStr)}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:2px; gap:20px;">
                        <span>Sales Velocity:</span> 
                        <span><b style="color:#5470C6;">${velocity}</b><span style="font-weight: 400;"> / month</span></span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:2px; gap:20px;">
                        <span>Stock Count:</span> 
                        <span><b style="color:#FF9F7F;">${stockQty}</b><span style="font-weight: 400;"> pcs</span></span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:2px; gap:20px;">
                        <span>Months of Supply:</span> 
                        <span><b style="color:#5470C6;">${mos}</b><span style="font-weight: 400;"> months</span></span>
                    </div>
                `;
            }
        """
    options = {
        "legend": {
            "data": ["Others", "Out of Stock", "Critical"],
            "itemWidth": 14,
            "itemHeight": 14,
            "top": "0%"
        },
        "tooltip": {
            "trigger": "item",
            "formatter": JsCode(stock_ledger_js),
            "backgroundColor": "rgba(255, 255, 255, 0.85)",
        },
        "grid": {
            "left": "0%", 
            "right": "1%", 
            "top": "10%", 
            "bottom": "0%", 
            "containLabel": False
        },
        "xAxis": {
            "name": "Days of Supply", 
            "type": "value",
            "max": max_x,
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "axisLabel": {
                "showMaxLabel": False,
                "color": "#596688",
                "fontSize": 12,
                "lineHeight": 20,
                "hideOverlap": True
            },
            "splitLine": { "lineStyle": { "type": [4, 5], "color": "rgba(0,0,0,0.1)" }},
            #region Name position
            "nameLocation": "end",  # KHÔNG THAY ĐỔI
            "nameGap": 30,          # KHÔNG THAY ĐỔI
            "nameTextStyle": {
                "verticalAlign": "top", # KHÔNG THAY ĐỔI
                "align": "right",   # KHÔNG THAY ĐỔI
                "padding": [30, 20, 0, 0] # KHÔNG THAY ĐỔI
            },
            #endregion
        },
        "yAxis": {
            "name": "Sales Velocity (1 Month)", 
            "type": "value",
            "min": 0,
            "max": max_y,
            "interval": y_interval,
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "splitLine": { "lineStyle": { "type": [4, 5], "color": "rgba(0,0,0,0.1)" }},
            "nameLocation": "end",
            "nameGap": 20,
            "axisLabel": {
                "color": "#596688",
                "fontSize": 12,
                "lineHeight": 20,
                "hideOverlap": True,
                "showMinLabel": False,
                "formatter": JsCode("""
                    function (value) {
                        if (value === 0) {
                            return '0';}
                        if (value < 1) {
                            return value.toFixed(1);}
                        return value.toFixed(0);}
                """)
            },
            "nameTextStyle": {
                "padding": [0, 0, 0, 110],
            }
        },
        "series": [
            {   
                "id": "others",
                "name": "Others",
                "type": "scatter",
                "data": chart_data["others"],
                "symbol": "circle",
                "itemStyle": {"color": "#6B9BE7"},
                "emphasis": {
                    "focus": "self",
                    "scale": True
                },
                "blur": { "itemStyle": {"opacity": 0.42}}
            },
            {   
                "id": "oos",
                "name": "Out of Stock",
                "type": "scatter",
                "data": chart_data["out_of_stock"],
                "symbol": "path://M19,6.41L17.59,5L12,10.59L6.41,5L5,6.41L10.59,12L5,17.59L6.41,19L12,13.41L17.59,19L19,17.59L13.41,12L19,6.41Z",
                "itemStyle": {"color": "#FFC400"},
                "emphasis": {
                    "focus": "self",
                    "scale": True
                },
                "blur": { "itemStyle": {"opacity": 0.42}}
            },
            {   
                "id": "critical",
                "name": "Critical",
                "type": "scatter",
                "data": chart_data["critical"],
                "symbol": "circle",
                "itemStyle": {"color": "#EC4D2D"},
                "emphasis": {
                    "focus": "self",
                    "scale": True
                },
                "blur": { "itemStyle": {"opacity": 0.42}}
            }
        ],
        "dataZoom": [
            {   
                "brushSelect": False,   # tắt zoom brush + movehandle
                "show": True,
                "type": "slider",
                "handleSize": "180%",
                "borderColor": "transparent", # Tắt khung
                "showDetail": False, # không show số cạnh handle
                "width": 22,
                "startValue": 0,
                "yAxisIndex": [0],
                "filterMode": "empty",
                "showDataShadow": False,
                "backgroundColor": "transparent",
                "fillerColor": "transparent",
                "moveHandleSize": 10,
                "handleStyle": {
                    "color": "#DDEFFF",
                    "borderColor": "#5A94E8",
                    "borderWidth": 2,
                    "shadowColor": "rgba(0, 0, 0, 0.2)",
                    "shadowBlur": 6
                },
                "emphasis": {
                    "handleStyle": {
                        "color": "#2d8fff",        # Hover vào thì tay nắm đổi sang màu xanh đậm
                        "borderColor": "#1a66ff"
                    },
                    "moveHandleStyle": {
                        "color": "#2d8fff"
                    }
                },
                "right": "1%"
            }
        ],
    }
    st_echarts(options=options, height="420px", key='Ledger_Scatter')

@st.fragment
def interact_Combo(
    stock_ledger    : pd.DataFrame,
    scored_ledger   : pd.DataFrame,
    start_period    : pd.Timestamp,
    end_period      : pd.Timestamp,
    columns_config  : dict,
    date_col        : str = c.date,
    interact_col    : str = c.sku,
    highlight_col   : str = s.end,
    prod_name_col   : str = c.prod_name,
    table_key       : str = 'qwerty',
    table_height    : int = 500
    ):
    SS = st.session_state
    def blue_col_style(
        df, sku, val, color='#1F6FEB'
        ):
        styling = {'subset': [sku], 'font-weight': '450', 'color': color}
        highlight = {
            'max': {'subset': [val], 'axis': 0, 'color': '#C7DFC7'},
            'min': {'subset': [val], 'axis': 0, 'color': '#FFD6DA'}
            }
        return (
            df.style
            .set_properties(**styling)
            .highlight_max(**highlight['max'])
            .highlight_min(**highlight['min'])
            )
    def reset_selection():
        SS[table_key] = {'selection': {'rows': [], 'columns': [], 'cells': []}}
    get_selection = lambda key: SS.get(key, {}).get('selection', {}).get('cells', [])

    st.dataframe(
        blue_col_style(scored_ledger, interact_col, highlight_col),
        column_config  = columns_config,
        height         = table_height,
        selection_mode = 'single-cell',
        key            = table_key,
        on_select      = 'rerun'
        )
    
    if selection := get_selection(table_key):
        prod_sku = None
        row_idx, col_name = selection[0]
        if col_name == interact_col:
            prod_sku    = scored_ledger.at[row_idx, col_name]
            prod_name   = scored_ledger.at[row_idx, prod_name_col]
            dict_sku    = {interact_col: prod_sku}
            if prod_sku:
                as_of_df: pd.DataFrame = get_specific_inventory_as_of(
                    ledger_df       = stock_ledger,
                    start_period    = start_period,
                    end_period      = end_period,
                    date_col        = date_col,
                    first_num_col   = s.start,
                    last_num_col    = highlight_col,
                    tree_event      = dict_sku
                )
                chart_data: dict       = get_stock_movement(as_of_df)
                @st.dialog(prod_name, width='large', on_dismiss=reset_selection)
                def show_dialog_chart(data, key):
                    tree_event_hyper_chart(data, f'dialog_{key}', height=550)
                show_dialog_chart(data = chart_data, key  = table_key)

#endregion

#region automate 🤖
def automate_chart_data(
        *,
        df      : pd.DataFrame,
        x_col   : str  = c.date,
        y_cols  : list = [c.revenue, c.qty],
        legends : list = ['Revenue', 'Quantity'],
        units   : list[Union[Literal['qty', 'pct', 'decimal', 'vnd', 'kg', '...'], str]] = ['vnđ', 'qty'],
        types   : list = ['bar', 'line'],
        axs_idx : list = [0, 1],
        colors  : list = None
        ):
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise ValueError('Dataframe is empty or invalid.')
    if len({len(legends), len(y_cols), len(types), len(units), len(axs_idx)}) > 1:
        raise ValueError('Length of legends, y_cols, types, and units must be equal.')
    if len(legends) != len(set(legends)):
        raise ValueError('Legends name must be unique.')
    if not all(df[y].dtypes.kind in 'iufc' for y in y_cols):
        raise ValueError('y_cols must be number format.')
    
    require_cols = [x_col, *y_cols]
    # Test
    config = df[require_cols].groupby(x_col, as_index=False)[y_cols].sum().tail(31).copy()
    colors = colors or [
        '#A4C9E8', '#F6BEB9', '#92C2C2', '#CEBBF3', "#B6C3CD",
        "#A5D1A5", "#ABC1E6", "#A7D7C8", "#CACEFF", "#FCE3AD"
    ]
    params = {
        'core' : {
            "name": "",
            "type": "bar",
            "yAxisIndex": 0,
            "xAxisIndex": 0,
            "silent": False,
            "datasetIndex": 0,
            "dimensions": None,
            "encode": None,
            "emphasis": {
                "focus": "none",
                "blurScope": "coordinateSystem",
                "disabled": False,
            }
        },
        'bar'  : {
            "barMinWidth": 2.5,
            "barMaxWidth": 20,
            "barGap": "20%",
            "barCategoryGap": "30%",
            "large": True,
            "largeThreshold": 1000,
            "sampling": "lttb",
            "universalTransition": True,
            "itemStyle": {
                "color": None,
                "borderRadius": 0,
                "opacity": 1,
                "borderWidth": 0,
                "borderColor": "#000",
                "borderType": "solid",
                "decal": None,
            },
            "emphasis": {
                "focus": "series",
                "blurScope": "coordinateSystem", 
                "itemStyle": {
                "opacity": 1,
                "borderWidth": 7,
                "borderColor": None,
                "shadowOffsetX": 0,
                "shadowOffsetY": 0,
                "shadowBlur": 0,
                }
            },
        },
        'line' : {
            "smooth": 0.2,
            "smoothMonotone": None,
            "showSymbol": False,
            "triggerLineEvent": False,
            "step": False,
            "connectNulls": False,
            "lineStyle": {
                "width": 3,
                "color": None,
                "type": "solid",
                "opacity": 1,
                "dashOffset": 0,
                "cap": "butt",
                "join": "bevel",
            },
            "emphasis": {
                "lineStyle": {
                    "width": 3,
                    "type": "solid",
                    "opacity": 1,
                    "dashOffset": 0,
                },
                "itemStyle": {
                    "color": None,
                    "borderColor": None,
                    "borderWidth": 0,
                    "borderType": "solid",
                }
            }
        }
    }

    x_dtype = config[x_col].dtypes.kind
    if x_dtype == 'M':
        config[x_col] = config[x_col].dt.strftime('%a %d\n%b').str.upper()
    elif x_dtype != 'O':
        config[x_col] = config[x_col].fillna('').astype(str)

    data = {
        series: config[[x_col, col_y]].values.tolist()
        for series, col_y 
        in zip(legends, y_cols)
    }
    z = list(reversed(range(len(legends))))
    series_list = [
        {
            **params["core"],
            "name": s,
            "type": types[i],
            "data": data[s],
            "z"   : z[i],
            "yAxisIndex": axs_idx[i],
            **params[types[i]],

            "itemStyle": {
                "borderRadius": [2, 2, 0, 0],
                "opacity": 0.95,
                "borderWidth": 1,
                "borderColor": colors[i % len(colors)],
            }
            if types[i] == "bar" else {},

            "lineStyle": {
                "width": 2,
                "type": "solid",
                "opacity": 0.8
            }
            if types[i] == "line" else {},
        }

        for i, s in enumerate(legends)
    ]

    key = f'ultimate_chart_{'_'.join(legends).lower()}'
    return series_list, {'colors': colors, 'units': units, 'key': key}

@st.fragment
def automate_chart(test_ultimate, colors: list, units: list, key: str='ultimate_v0'):
    units_json = json.dumps(units)

    tooltip_formatter = JsCode(f"""
            function(params) {{
                const units = {units_json};

                let formattedDate = params[0].name;
                let res = '<div style="font-size:14px; font-weight:bold; margin-bottom:10px;">' + formattedDate + '</div>';

                params.forEach(item => {{
                    let val = Array.isArray(item.value) ? item.value[1] : item.value;
                    if (val === null || val === undefined) return;
                    
                    let idx = item.seriesIndex;
                    // Nếu units[idx] không tồn tại, mặc định là 'qty'
                    let type = (units && units[idx]) ? units[idx] : 'qty'; 
                    
                    let displayVal = '';

                    if (type === 'pct') {{
                        // Định dạng phần trăm
                        displayVal = (val * 100).toFixed(1) + '%';
                    }} else if (type === 'decimal') {{
                        // Định dạng số thập phân (2 chữ số)
                        displayVal = Number(val).toLocaleString(undefined, {{
                            minimumFractionDigits: 2, 
                            maximumFractionDigits: 2
                        }});
                    }} else if (type === 'qty') {{
                        // Định dạng số nguyên thuần túy
                        displayVal = Math.round(val).toLocaleString();
                    }} else {{
                        // (VNĐ, kg, USD...) 
                        // Tự hiểu là qty + suffix
                        displayVal = Math.round(val).toLocaleString() + ' ' + type;
                    }}
                    res += '<div style="display:flex;justify-content:space-between;gap:20px; margin-bottom: 6px;">' +
                        '<span style="font-weight:400;">' + item.marker + item.seriesName + '</span>' + 
                        '<span style="font-weight:550; font-variant-numeric:tabular-nums;\
                            font-family: "JetBrains Mono", "Roboto Mono";\
                            ">' + displayVal + '</span>' + 
                        '</div>';
                }});

                return res;
            }}
        """)
    y_label_formatter = JsCode("""
        function(value) {
            var absVal = Math.abs(value);
            var sign = value < 0 ? '-' : '';
            if (absVal >= 1000000000) return sign + parseFloat((absVal / 1000000000).toFixed(1)) + 'B';
            if (absVal >= 1000000) return sign + parseFloat((absVal / 1000000).toFixed(1)) + 'M';
            if (absVal >= 1000) return sign + parseFloat((absVal / 1000).toFixed(1)) + 'k';
            return value % 1 === 0 ? value.toLocaleString() : value.toFixed(1);
        }
    """)
    options = {
        "color": colors,
        "animation": True,
        "animationDuration": 350,
        "animationDurationUpdate": 500,
        "animationEasingUpdate": "cubicOut",
        "backgroundColor": 'transparent',
        "tooltip": {
            "trigger": 'axis',
            "confine": True,
            "backgroundColor": 'rgba(255, 255, 255, 0.95)',
            "formatter": tooltip_formatter
        },
        "legend": {
            "width": "80%",
            "top": 5, 
            "icon": 'circle',
            "textStyle": {
                "fontSize": 12,
            }
        },
        "grid": {
            "left": '10', "right": '10', "bottom": '0', "top": '15%', "containLabel": False 
        },
        "xAxis": {
            "type": "category",
            "boundaryGap": True,
            "axisLabel": {
                "color": '#999',
                "fontSize": 11,
                "lineHeight": 14,
                "hideOverlap": True,
                "interval": 6
            },
            "axisLine": { "lineStyle": { "color": '#eee' } }
        },
        "yAxis": [
            {
                "type": 'value',
                "axisLabel": { "showMinLabel": False, "color": '#999', "fontSize": 12, "formatter": y_label_formatter },
                "splitLine": { "lineStyle": { "type": [4, 5], "color": "rgba(0,0,0,0.075)" }},
            },
            {
                "type": 'value',
                "splitLine": { "show": False },
                "axisLabel": { "showMinLabel": False, "color": '#AAA', "fontSize": 11, "formatter": y_label_formatter }
            }
        ],
        "series": test_ultimate,
        "toolbox": {
            "show": True,
            "showTitle": False,
            "tooltip": {
                "show": True,
                "position": "top",
                "textStyle": {
                    "fontSize": 14
                }
            },
            "feature": {
                "magicType": {"show": True, "type": ["stack"]},
            }
        }
    }
    st_echarts(options=options, height="500px", key=key)
#endregion

#region #* Dynamic Everything

def get_dynamic_mask(  
    # @No Cache vì return Lambda streamlit không hash nổi
    df              : pd.DataFrame, 
    start_anchor    : pd.Timestamp,
    _date           : str = c.date,
    period_mode     : Literal['date', 'week', 'month_year'] = c.week
):
    """
    ### Dynamic_Mask là function apply start/end date
    ### lên bất kỳ dataframe nào có cột date tương ứng
    # -
    ### Helper của get_dynamic_line_data or whaterver need it
    Xử lý logic dịch chuyển điểm neo (Start/End) theo chu kỳ và đóng gói thành Filter Mask.

    Mechanism:
    >>> 1. Sync Logic: 
        - Nếu là 'week': Tự động tìm Monday gần nhất với start_anchor và Sunday gần nhất với end_anchor 
          có tồn tại trong dữ liệu để đảm bảo biểu đồ không bị "gãy" ở hai đầu.
        - Nếu là 'month_year': Ép start_anchor về ngày đầu tháng (Mùng 1).
    >>> 2. Validation: Sử dụng `.any()` thay vì `set()` để kiểm tra sự tồn tại của mốc thời gian, 
          tối ưu bộ nhớ cho DataFrame lớn.
    >>> 3. Closure: Trả về một lambda function đã đóng gói (capture) các giá trị anchor và tên cột `_date`.

    Args:
    >>> df (pd.DataFrame): DataFrame chứa cột thời gian để thực hiện validation mốc tồn tại.
    >>> start_anchor (pd.Timestamp): Điểm neo bắt đầu dự kiến.
    >>> _date (str): Tên cột dữ liệu thời gian (mặc định 'date').
    >>> period_mode (str): Chế độ gom nhóm thời gian ('date', 'week', 'month_year').

    Returns:
    >>> Callable[[pd.DataFrame], pd.Series]: Một mask function có thể truyền trực tiếp vào `.loc` 
        của bất kỳ DataFrame nào có cùng cấu trúc cột thời gian.
    >>> period_mode: để đồng bộ mask & data trả về
    """
    if _date not in df.columns:
        print(f"[get_dynamic_mask] Column {_date} does not exist")
        return None
    
    # Đồng bộ format & lấy các mốc thời gian từ Series trực tiếp
    # start_anchor nên được ép kiểu bên ngoài hoặc truyền vào là Timestamp
    end_anchor = df[_date].max()

    if start_anchor is not None:
        if period_mode == c.week:
            monday_offset = pd.offsets.Week(weekday=0)
            sunday_offset = pd.offsets.Week(weekday=6)
            next_monday = start_anchor + monday_offset
            last_monday = start_anchor - monday_offset
            last_sunday = end_anchor - sunday_offset
        elif period_mode == c.month:
            begin_of_curr_m = start_anchor.replace(day=1)
            end_of_last_m   = end_anchor - pd.offsets.MonthEnd(1)

    if period_mode == c.week:
        # if start_anchor not Monday:       # Not Zero (Monday)
        if start_anchor.dayofweek != 0:     #* Ưu tiên if Last Monday
            # Thay vì dùng set, kiểm tra trực tiếp trên Series (hiệu quả với dữ liệu lớn qua isin/any)
            if (df[_date] == last_monday).any():   # if Last Monday exist
                start_anchor = last_monday
            elif (df[_date] == next_monday).any(): # if Next Monday exist
                start_anchor = next_monday 

        # if end_anchor not Sunday:         # Not Six (Sunday)
        if end_anchor.dayofweek != 6:       #* Không lấy đc Next Sunday (source không design để lấy dư)
            if (df[_date] == last_sunday).any():   # if Last Sunday exist
                end_anchor = last_sunday

    elif period_mode == c.month:
        # Chỉ cần start_anchor always = Mùng 1
        # ignore if 'begin_of_month' not exist in dates_data
        if not start_anchor.is_month_start:
            if (df[_date] == begin_of_curr_m).any():
                start_anchor = begin_of_curr_m

        if not end_anchor.is_month_end:
            if (df[_date] == end_of_last_m).any():
                end_anchor = end_of_last_m

    period_mask = lambda x: (start_anchor <= x[_date]) & (x[_date] <= end_anchor)

    return period_mask, period_mode

@st.cache_data
def get_dynamic_agg_pivot(
    df_double    : pd.DataFrame,
    _dynamask    : Callable[[pd.DataFrame], pd.Series], # vì cache_data nên phải thêm _ để không bị lỗi vì mask là lambda
    widget_key   : str, # dùng khi cần force cache refresh data (chưa chắc chắc cần thiết không)
    _groupby     : str  = c.staff,
    agg_config   : dict = {c.revenue: 'sum', c.invoice: 'nunique'},
    period_mode  : Literal['date', 'week', 'month_year'] = c.week,
    y_limit      : int = 10
    ):
    """
    ## Hàm tạo Multi-Dimension Pivot Table
    ### Phục vụ mục đích tính toán chỉ số kinh doanh bằng cách nhân chia các 2D-pivot.
    """
    if not isinstance(df_double, pd.DataFrame) or df_double.empty:
        return
    if not isinstance(_groupby, str) or df_double[_groupby].dtypes.kind == 'M': 
        return
    

    dynamic_mask = _dynamask
    val_cols     = list(agg_config)
    pre_groupbys = [period_mode, _groupby]


    #? Thu gọn data trước pivot, USE: agg_config
    if  (df_masked:= 
        (df_double.loc[dynamic_mask]
        .groupby(pre_groupbys, observed=True) # Added observed
        .agg(agg_config)
        .reset_index()
        )
        ).empty: return
    
    string_col = df_masked.select_dtypes(include=('string', 'object')).columns
    string_col = [col for col in string_col if col not in val_cols]
    df_masked[string_col] = df_masked[string_col].astype('category')


    top_entities = (
        df_masked.groupby(_groupby, observed=True)
        #? Lấy top thực thể, USE value.sum()
        [val_cols].sum()
        .sort_values(val_cols[0], ascending=False)
        .head(y_limit)
        .index
        .remove_unused_categories() # Xóa tên mấy ông bị loại cho clean
        )

    _note = """
        ! n = len([agg_config])
        ? Mục đích pivot:
        - Tạo ra 1 cuốn sổ có 'number of Page' = 'n'
        - là một khối 3D (Time x Category x Metric).

        * 1 Page = pivot 2D (Time x Category) đại diện 1 Metric
        + trục Y = Time (period_mode)
        + trục X = Category (_groupby.unique())
        + Metric = agg_col.agg_method() *groupby ở df_masked
        """ 

    df_masked    = df_masked.loc[df_masked[_groupby].isin(top_entities), :]
    pivot_table  = (pd.pivot_table(
        data     = df_masked,
        values   = val_cols,
        index    = period_mode,
        columns  = _groupby,
        aggfunc  = 'sum', # Chỗ này dùng cố định 'sum'
        observed = True)
        .fillna(0)
        )
    # print(pivot_table)
    if not pivot_table.empty:
        # CategoricalIndex -> List
        top_entities = list(top_entities)
        return {key: pivot_table[key][top_entities] for key in val_cols}

@st.cache_data
def get_dynamic_dataframe(
        df_double   : pd.DataFrame,
        _options    : dict,
        option_key  : str,
        _dynamask   : Callable[[pd.DataFrame], pd.Series],
        period_mode : str,
        y_limit     : int = 10):
    """
    ### Get Dynamic Dataframe
    - Cầm KEY mở lấy config từ Options
    - Gọi `get_dynamic_agg_pivot()`
    - Tự tính ra df final từ các pivot_df
    """
    if not isinstance(df_double, pd.DataFrame) or df_double.empty or _options is None:
        return

    if option_key not in _options:
        option_key = list(_options)[0]
    pivot_config = _options[option_key]
    

    pivot_dict = (
        get_dynamic_agg_pivot(
        df_double   =   df_double,
        _dynamask   =   _dynamask,
        widget_key  =   f"pvt_{option_key}_{period_mode}",
        _groupby    =   pivot_config["groupby"],
        agg_config  =   pivot_config["agg_config"],
        period_mode =   period_mode,
        y_limit     =   y_limit
        )
    )

    if not pivot_dict: return
    res_keys = list(pivot_dict.keys())

    if len(res_keys) == 1:
        pivot_df = pivot_dict[res_keys[0]]
    
    elif len(res_keys) >= 2:
        pivot_df = pivot_config["calc"](*pivot_dict.values())

    
    if not isinstance(pivot_df, pd.DataFrame) or pivot_df.empty:
        return
    else:
        # Đổi tên cột 'store_id' thành tên chỉ số
        if len(pivot_df.columns) == 1:
            _, dot, _tail = option_key.partition('•')
            try:
                _name, _ = _tail.split(' (')
                pivot_df.columns = [_name]
            except: # Fallback: when use advanced filter to view 1 subject
                pass

        pivot_df = pivot_df.loc[:, pivot_df.sum(axis=0) >= 0]

        return pivot_df.astype('float').fillna(0.0).round(pivot_config.get("round", 1))


def pivot_bar_data(
    pivot_input : pd.DataFrame,
    period_mode : Literal['date', 'week', 'month_year'],
    units       : Literal['qty', 'pct', 'decimal', 'vnd', 'kg', '...'] = 'vnd',
    colors      : list = None,
    ):
    """
    ## Hàm nhận 1 pivot table đã qua tính toán và thực hiện tạo dataset, series ready for echart. 
    - .dt:  chỉ Series chứa dữ liệu thời gian.
    - .str: chỉ Series chứa dữ liệu văn bản.
    - .cat: chỉ Series chứa dữ liệu phân loại.    
    """
    if not isinstance(pivot_input, pd.DataFrame) or pivot_input.empty:
        return

    pivot    = pivot_input.reset_index(drop=False).copy()
    params   = {
        'core' : {
            "name": "",
            "type": "bar",
            "yAxisIndex": 0,
            "xAxisIndex": 0,
            "silent": False,
            "datasetIndex": 0,
            "dimensions": None,
            "encode": None,
            "emphasis": {
                "focus": "none",
                "blurScope": "coordinateSystem",
                "disabled": False,
            }
        },
        'bar'  : {
            "barMinWidth": 2,
            "barMaxWidth": 18,
            "barGap": "15%",
            "barCategoryGap": "25%",
            "large": True,
            "largeThreshold": 1000,
            "sampling": "lttb",
            "universalTransition": True,
            "itemStyle": {
                "color": None,
                "borderRadius": [9, 9, 0, 0],
                "opacity": 0.95,
                "borderWidth": 0.71,
                "borderColor": "rgba(255, 255, 255, 0.3)",
                "borderType": "solid",
                "decal": None,
            },
            "emphasis": {
                "focus": "series",
                "blurScope": "coordinateSystem",
                "itemStyle": {
                    "opacity": 1,
                    "borderWidth": 5,
                    "borderColor": "rgba(0, 0, 0, 0.05)",
                    "shadowOffsetX": 0,
                    "shadowOffsetY": 0,
                    "shadowBlur": 0,
                }
            }
        }
    }
    times    = {
        c.date      : lambda x: x.dt.strftime('%a %d\n%b'),
        c.week      : lambda x: x,
        c.month     : lambda x: pd.to_datetime(x, format=f.month, errors='coerce').dt.strftime('%b. %y')
    }
    pivot[period_mode] = times[period_mode](pivot[period_mode])
    headers  = pivot.columns.to_list()
    data     = [headers] + pivot.values.tolist()
    legends  = headers[1:]
    units    = [units] * len(legends)
    interval = (6 if period_mode == c.date else 'auto')
    monotone = (["#B6C3CD"] if len(legends) == 1 else None)
    colors   = monotone or colors or [
        '#A4C9E8', '#F6BEB9', '#92C2C2', '#CEBBF3', "#B6C3CD",
        "#A5D1A5", "#ABC1E6", "#A7D7C8", "#CACEFF", "#FCE3AD"
        ]
    z        = list(reversed(range(len(legends))))
    series_list = [
        {
            **params["core"],
            "name": s,
            "type": "bar",
            "z"   : z[i],
            "yAxisIndex": 0,
            **params["bar"]
        }
        for i, s in enumerate(legends)
    ]

    return {
        'data'     : {'source': data}, 
        'series'   : series_list, 
        'units'    : units,
        'colors'   : colors,
        'interval' : interval
        }

@st.fragment
def pivot_bar(
    data     : dict,
    series   : list,
    units    : list,
    colors   : list,
    interval : None,
    key      : str = 'ultimate_v0',
    height   : int = 360
    ):
    y_label_unit = units[0]
    tooltip_for_dataset = JsCode(f"""
            function(params) {{
                const units = {units};

                let formattedDate = params[0].name;
                let res = '<div style="font-size:14px; font-weight:bold; margin-bottom:10px;">' + formattedDate + '</div>';

                params.forEach(item => {{
                    let val = Array.isArray(item.value) ? item.value[item.seriesIndex + 1] : item.value;
                    if (val === null || val === undefined) return;
                    
                    let idx = item.seriesIndex;
                    // Nếu units[idx] không tồn tại, mặc định là 'qty'
                    let type = (units && units[idx]) ? units[idx] : 'qty'; 
                    
                    let displayVal = '';

                    if (type === 'pct') {{
                        // Định dạng phần trăm
                        displayVal = (val * 100).toFixed(1) + '%';
                    }} else if (type === 'decimal') {{
                        // Định dạng số thập phân (2 chữ số)
                        displayVal = Number(val).toLocaleString(undefined, {{
                            minimumFractionDigits: 2, 
                            maximumFractionDigits: 2
                        }});
                    }} else if (type === 'qty') {{
                        // Định dạng số nguyên thuần túy
                        displayVal = Math.round(val).toLocaleString();
                    }} else {{
                        // (VNĐ, kg, USD...) 
                        // Tự hiểu là qty + suffix
                        displayVal = Math.round(val).toLocaleString() + ' ' + type;
                    }}
                    res += '<div style="display:flex;justify-content:space-between;gap:20px; margin-bottom: 6px;">' +
                        '<span style="font-weight:400;">' + item.marker + item.seriesName + '</span>' + 
                        '<span style="font-weight:550; font-variant-numeric:tabular-nums;\
                            font-family: "JetBrains Mono", "Roboto Mono";\
                            ">' + displayVal + '</span>' + 
                        '</div>';
                }});

                return res;
            }}
        """)
    y_label_for_dataset = JsCode(f"""
        function(value) {{
            const unitType = '{y_label_unit}';
            
            if (unitType === 'pct') {{
                return (value * 100).toFixed(0) + '%';
            }}

            var absVal = Math.abs(value);
            var sign = value < 0 ? '-' : '';
            if (absVal >= 1000000000) return sign + parseFloat((absVal / 1000000000).toFixed(1)) + 'B';
            if (absVal >= 1000000) return sign + parseFloat((absVal / 1000000).toFixed(1)) + 'M';
            if (absVal >= 1000) return sign + parseFloat((absVal / 1000).toFixed(1)) + 'k';
            return value % 1 === 0 ? value.toLocaleString() : value.toFixed(1);
        }}
    """)
    options = {
        "dataset": data,
        "color": colors,
        "animation": True,
        "animationDuration": 350,
        "animationDurationUpdate": 500,
        "animationEasingUpdate": "cubicOut",
        "backgroundColor": 'transparent',
        "tooltip": {
            "trigger": 'axis',
            "confine": True,
            "backgroundColor": 'rgba(255, 255, 255, 0)',
            "formatter": tooltip_for_dataset
        },
        "legend": {
            "width": "80%",
            "top": 5, 
            "icon": 'circle',
            "textStyle": {
                "fontSize": 12,
            }
        },
        "grid": {
            # Đồng bộ thực tế với lề của hero chart
            "left": '2.5%', "right": '2.5%', "bottom": '0', "top": '15%', "containLabel": False 
        },
        "xAxis": {
            "type": "category",
            "boundaryGap": True,
            "axisLabel": {
                "color": '#999',
                "fontSize": 12,
                "lineHeight": 14,
                "hideOverlap": True,
                "interval": interval
            },
            "axisLine": { "lineStyle": { "color": '#eee' } }
        },
        "yAxis": [
            {
                "type": 'value',
                "axisLabel": { "showMinLabel": False, "color": '#999', "fontSize": 12, "formatter": y_label_for_dataset },
                "splitLine": { "lineStyle": { "type": [4, 5], "color": "rgba(0,0,0,0.075)" }},
            },
            {
                "type": 'value',
                "splitLine": { "show": False },
                "axisLabel": { "showMinLabel": False, "color": '#AAA', "fontSize": 11, "formatter": None }
            }
        ],
        "series": series,
        "toolbox": {
            "show": True,
            "showTitle": False,
            "tooltip": {
                "show": True,
                "position": "top",
                "textStyle": {
                    "fontSize": 14
                }
            },
            "feature": {
                "magicType": {"show": True, "type": ["stack"]},
            }
        }
    }

    st_echarts(options=options, height=f"{height+40}px", key=key)

#endregion

