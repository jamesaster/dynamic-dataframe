import streamlit as st
import pandas as pd
import numpy as np
import inspect
import re

#region helper
GRANULARITY_MAP = { 
    'Day':   {'col': 'date',       'title': 'Daily'}, 
    'Week':  {'col': 'week',       'title': 'Weekly'}, 
    'Month': {'col': 'month_year', 'title': 'Monthly'}
}
TABLE_FORMATTER = {
    'VNĐ': lambda x: (
        f"{x/1e9:,.1f} B" if abs(x) >= 1e9  
        else f"{x/1e6:,.1f} M" if abs(x) >= 1e6  
        else f"{x/1e3:,.0f} k" if abs(x) >= 1e3  
        else f"{x:,.0f}"
    ) if pd.notnull(x) else "-",
    'pct': lambda x: f"{x*100:.1f}%" if pd.notnull(x) else "0.0%",
    'qty': lambda x: f'{int(x):,}' if pd.notnull(x) else "0",
    'decimal': lambda x: f'{x:,.1f}' if pd.notnull(x) else "0.0",
}

def CSS_custom_blue_tips(
        x='Click', y='a blue', z='to do something', 
        align='right', px=8, down=70, icon='touch_app'
        ):
    st.markdown(f"""
    <div style="display:flex; align-items:'center'; position:absolute; {align}: {px}px;
        margin-top:{down}px; color:#8891aa; font-size:0.7rem; white-space:nowrap;">
        <span class="material-symbols-outlined" style="font-size:0.9rem; margin-right:5px">{icon}</span>
        <span style="letter-spacing:0.40px">{x} <b style="color:#5A94E8"> {y} </b> {z} </span> 
    </div>
    """, unsafe_allow_html=True
    )
def custom_sort(char):
    """
    Sắp xếp theo Prefix: IPH > I > Alphabet > A > Number
    """
    if not isinstance(char, str) or not char: return (5, char)
    c = char.upper()
    
    if c.startswith('TOT'): return (0, char)
    if c.startswith('IPH'): return (1, char)
    if c.startswith('I'):   return (2, char)
    if c.startswith('A'):   return (4, char) # Đẩy A xuống dưới Alphabet
    if c[0].isalpha():      return (3, char) # Các chữ cái khác (B, C, D...)
    
    return (5, char)
def get_fade_color(hex_or_rgba, opacity=0.5):
    color_str = str(hex_or_rgba).strip()
    
    # Nếu rgba, trả về nguyên xi
    if color_str.lower().startswith('rgba'):
        return color_str
        
    # Xử lý mã HEX
    hex_color = color_str.lstrip('#')
    rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    return f"rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, {opacity})"
def styled_header(text, subtext = '', h :int = 3):
    style_main = "text-align: left; color: rgba(52, 67, 109, 0.95);"
    style_sub  = "margin-left: 10px; font-weight: 400; color: rgba(52, 67, 109, 0.65); font-size: 27px;"
    html = f"""
            <h{h} style='{style_main}'>
                {text} <span style='{style_sub}'>{subtext}</span>
            </h{h}>
            """
    return st.markdown(html, unsafe_allow_html=True)
def format_number(n):
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B"
    elif n >= 1_000_000:
        return f"{n / 1_000_000:.0f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)
def JAMES_THEME():
    return {
    "color": ["#6081dd", "#62b9b9", "#ffba66", "#7cbbfa", "#ff8894", "#82c082", "#a4b0be"],
    "backgroundColor": "transparent", # Giữ trong suốt để linh hoạt với UI
    
    # Cấu hình Typography
    "textStyle": {
        "fontFamily": "'SF Pro Display', 'Helvetica Neue', 'Inter', 'Segoe UI', Roboto, Arial, sans-serif"
    },
    
    # Cấu hình Title (Dùng cho Pie chart của bạn)
    "title": {
        "textStyle": {"fontSize": 14, "fontWeight": "600", "color": "#4a69bd"},
        "subtextStyle": {"fontSize": 28, "fontWeight": "450", "color": "#4a69bd"}
    },

    # Cấu hình Legend
    "legend": {
        "icon": "circle",
        "textStyle": {"color": "#666"}
    },

    # Cấu hình Trục tọa độ (X-axis)
    "categoryAxis": {
        "axisLine": {"lineStyle": {"color": "#eee"}},
        "axisTick": {"show": False},
        "axisLabel": {"color": "#999", "fontSize": 12},
        "splitLine": {"show": False}
    },
    
    # Cấu hình Trục giá trị (Y-axis)
    "valueAxis": {
        "axisLine": {"show": False},
        "axisLabel": {"showMinLabel": False, "color": "#666", "fontSize": 12},

        "splitLine": {
            "show": True,
            "lineStyle": {"type": [4, 5], "color": "rgba(0, 0, 0, 0.075)"}
        }
    },

    # Kiểu dáng cho Line Chart
    "line": {
        "smooth": 0.2, # Độ cong nhẹ bạn hay dùng
        "showSymbol": False,
        "lineStyle": {"width": 2.5}
    },

    # Kiểu dáng cho Pie Chart
    "pie": {
        "itemStyle": {
            "borderRadius": 10,
            "borderColor": "#ffffff",
            "borderWidth": 2.5
        },
        "label": {"show": True, "position": "outside"},
        "labelLine": {"length": 10, "length2": 15, "smooth": True}
    },

    # Tooltip mặc định mờ ảo
    "tooltip": {
        "backgroundColor": "rgba(255, 255, 255, 0.9)",
        "borderColor": "#eee",
        "borderWidth": 1,
        "textStyle": {"color": "#3A3A3A", "fontSize": 14}, # "fontWeight": 500
        "axisPointer": {
            "lineStyle": {"color": "#ccc", "width": 1}
        }
    }
    }   

def format_metric_number(val):
    """
    ### Định dạng số (for hiển thị only)
    - 1_000_000_000 = 1.0B
    - 1_000_000     = 1.0M
    - 1_000         = 1.0k
    """
    if val >= 1_000_000_000:
        return f"{val / 1_000_000_000:.2f}B"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.2f}M"
    if val >= 1_000:
        return f"{val / 1_000:.1f}k"
    return f"{val:.2f}"
def get_sparkline(data: pd.Series, max_points=30):
    """
    ### Down Sampling by Linspace: `pd.Series`
    #### Phục vụ hiển thị sparkline với số lượng điểm chính xác.
    """
    if data.empty:
        return []
        
    n = len(data)
    if n <= max_points:
        return data.tolist()
    
    # Linspace đảm bảo luôn lấy đúng số lượng max_points
    indices = np.linspace(0, n - 1, num=max_points).astype(int)
    
    return data.iloc[indices].tolist()


def clear_attrs(df):
    """Xóa sạch attrs để Streamlit không báo lỗi Timestamp"""
    df.attrs = {}
    return df
def sync_event(
    df_period_regular       : pd.DataFrame,
    _date                   : str,
    today                   : pd.Timestamp,
    finder_event_flag_key   : str,
    raw_main_event_key      : str,
    finder_date_key         : str
    ):
    SS = st.session_state
    event_date = today
    valid_date = df_period_regular[_date]
    # Mỗi lần chỉ khởi tạo đc 1 level cho SS.key
    if 'chart_event' not in SS[raw_main_event_key]:
        SS[raw_main_event_key].setdefault('chart_event', {}).setdefault('date', {})


    if SS.get(finder_event_flag_key) == 'Synced':
        if pd.notna(
            VALIDATED_finder_event := 
                pd.to_datetime(
                    SS.get(finder_date_key),dayfirst=True,errors='coerce')
            ):
            if (VALIDATED_finder_event == valid_date).any():
                SS[raw_main_event_key]['chart_event'] = {'date': VALIDATED_finder_event}



    if (str_main_event :=
            SS[raw_main_event_key]
                and SS[raw_main_event_key].get('chart_event', {})
                and SS[raw_main_event_key].get('chart_event', {}).get('date', None)
            ):
        if pd.notna(main_event := 
            pd.to_datetime(str_main_event, yearfirst=True, errors='coerce')
            ):
            event_date = main_event

    return event_date 
def get_source_code(func):
    if hasattr(func, "__wrapped__"):
        func = func.__wrapped__
    raw_source = inspect.getsource(func)
    source_no_comment = re.sub(r'(?<!["\'])#.*', '', raw_source)
    clean_source = re.sub(r'("""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\')', '', source_no_comment, count=1)

    return clean_source.strip()
def get_source_code_raw(func):
    if hasattr(func, "__wrapped__"):
        func = func.__wrapped__
    raw_source = inspect.getsource(func)

    clean_source = re.sub(
        r"^\s*(\"\"\"[\s\S]*?\"\"\"|'''[\s\S]*?''')\s*\n",
        "",
        raw_source,
        count=1,
        flags=re.M,
    )
    return clean_source.strip()


def custom_sort(char):
    """
    Sắp xếp theo Prefix: IPH > I > Alphabet > A > Number
    """
    if not isinstance(char, str) or not char: return (5, char)
    c = char.upper()
    
    if c.startswith('TOT'): return (0, char)
    if c.startswith('IPH'): return (1, char)
    if c.startswith('I'):   return (2, char)
    if c.startswith('A'):   return (4, char) # Đẩy A xuống dưới Alphabet
    if c[0].isalpha():      return (3, char) # Các chữ cái khác (B, C, D...)
    
    return (5, char)

#endregion