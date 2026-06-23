import pandas as pd
import streamlit as st
from src.columns import colName as c, stockCol as s
from visuals.visuals_helper import *
from visuals.e_charts import *
from src.stockledger import *

#region Helper
SS = st.session_state
get_selection = lambda key: SS.get(key, {}).get('selection', {}).get('cells', [])
columns_config = {
    s.prod_name         : st.column_config.TextColumn('Product', width=None),
    s.sku               : st.column_config.TextColumn('SKU (click to view)', width=None, alignment='center'),
    s.cat               : st.column_config.TextColumn('Category', width='small'),
    'since_last_import' : st.column_config.NumberColumn('Last import', format='%,dd', width='small'),
    'since_last_sales'  : st.column_config.NumberColumn('Last sales', format='%,dd', width='small'),
    s.end               : st.column_config.NumberColumn('End Stock', format='%,d', width='small'),
    'last_60d_sales'    : st.column_config.NumberColumn('60D Sales', format='%,d', width='small'),
    'demand_score'      : st.column_config.TextColumn('Demand', width='small'),
    'supply_score'      : st.column_config.TextColumn('Supply', width='small'),
    'month_velocity'    : st.column_config.NumberColumn('Mo. Velocity', format='%,d', width='small'),
    'days_of_cover'     : st.column_config.NumberColumn('Cover Days', format='%,dd', width='small'),
}
table_toolbar_off = """
    <style>
    .st-key-secondary_cont [data-testid="stElementToolbar"] { display: none !important;}
    </style>
    """
#endregion
#region # Options
_STORE       = 'store_id'
_SUBCAT      = 'sub_cat_manual'
_SUM         = 'sum'
_NUNIQUE     = 'nunique'
DYNAMIC_DATA_OPTIONS = {
# NOTE: Vì đã xử lý duplicate traffic nên dùng method SUM rất chuẩn (mỗi ngày 1 số traffic)
    #region #? Store
    'Store • Conversion Rate (CR)': {
        'groupby': _STORE,
        'agg_config': {c.invoice: _NUNIQUE, c.traffic: _SUM},
        'calc': lambda left, right: (left / right),
        'round': 3,
        'time': c.date,
        'units': 'pct'
    },
    'Store • Visitors Per Ticket (VPT)': {
        'groupby': _STORE,
        'agg_config': {c.traffic: _SUM, c.invoice: _NUNIQUE},
        'calc': lambda left, right: left / right,
        'time': c.date,
        'units': 'Visits'
    },
    'Store • Revenue Per Visitor (RPV)': {
        'groupby': _STORE,
        'agg_config': {c.revenue: _SUM, c.traffic: _SUM},
        'calc': lambda left, right: left / right,
        'time': c.date,
        'units': 'VNĐ'
    },
    'Store • Average Ticket Value (ATV)': {
        'groupby': _STORE,
        'agg_config': {c.revenue: _SUM, c.invoice: _NUNIQUE},
        'calc': lambda left, right: left / right,
        'time': c.date,
        'units': 'VNĐ'
    },
    'Store • Units Per Ticket (UPT)': {
        'groupby': _STORE,
        'agg_config': {c.qty: _SUM, c.invoice: _NUNIQUE},
        'calc': lambda left, right: left / right,
        'time': c.date,
        'units': 'decimal'
    },
    'Store • Average Selling Price (ASP)': {
        'groupby': _STORE,
        'agg_config': {c.revenue: _SUM, c.qty: _SUM},
        'calc': lambda left, right: left / right,
        'time': c.date,
        'units': 'VNĐ'
    },
    'Store • Devices Per Transaction (DPT)': {
        'groupby': _STORE,
        'agg_config': {c.imei_sn: _NUNIQUE, c.invoice: _NUNIQUE},
        'calc': lambda left, right: left / right,
        'time': c.date,
        'units': 'decimal'
    },
    #endregion
    #region #? Staff
    # --- Nhóm 1: Volume metrics
    'Staff • Performance (Revenue)': {
        'groupby': c.staff,
        'agg_config': {c.revenue: _SUM},
        'units': 'VNĐ'
    },
    'Staff • Devices Sold': {
        'groupby': c.staff,
        'agg_config': {c.imei_sn: _NUNIQUE},
        'units': 'Pcs'
    },
    'Staff • Transactions': {
        'groupby': c.staff,
        'agg_config': {c.invoice: _NUNIQUE},
        'units': 'Invoice'
    },
    # --- Nhóm 2: Efficiency metrics
    'Staff • Attachment Rate': {
        'groupby': c.staff,
        'agg_config': {
            c.qty    : _SUM,
            c.imei_sn   : _NUNIQUE
        },
        'calc': lambda qty, imei: (qty - imei) / imei,
        'units': 'pct'
    },
    'Staff • Average Ticket Value (ATV)': {
        'groupby': c.staff,
        'agg_config': {c.revenue: _SUM, c.invoice: _NUNIQUE},
        'calc': lambda left, right: left / right,
        'units': 'VNĐ'
    },
    'Staff • Units Per Transaction (UPT)': {
        'groupby': c.staff,
        'agg_config': {c.qty: _SUM, c.invoice: _NUNIQUE},
        'calc': lambda left, right: left / right,
        'units': 'decimal'
    },
    'Staff • Revenue Per Unit (RPU)': {
        'groupby': c.staff,
        'agg_config': {c.revenue: _SUM, c.qty: _SUM},
        'calc': lambda left, right: left / right,
        'units': 'VNĐ'
    },
    #endregion
    #region #? Others

    #endregion
}
#endregion


# colNamed
@st.fragment
def performance_and_target(
    df_stage_1_date  : pd.DataFrame,
    period_double    : pd.DataFrame,
    stock_ledger     : pd.DataFrame,
    period_anchor    : pd.Timestamp,
    today            : pd.Timestamp,

    rounded_month_df : pd.DataFrame,
    df_target        : pd.DataFrame,
    stage_1_dynamic  : pd.DataFrame,

    dict_stage_2     : dict,
    period_mode      : str = None,
    global_timemode  : str = None,
    time_mode_title  : str = None,
    month_year       : str = c.month,
    tab_key          : str = 'pfm_tab',
    ):

    df_double = period_double.copy()
    st.html(table_toolbar_off)

    #region # Default params 
    # for tab 1+2
    local_timemode      = global_timemode
    OPTION_KEY          = 'Staff • Performance (Revenue)'
    options_list        = list(DYNAMIC_DATA_OPTIONS)
    history_option_KEY  = tab_key + '_history_option'
    time_label          = {
        c.date: 'Daily',
        c.week: 'Weekly',
        c.month: 'Monthly'
        }
    local_timemode_KEY  = tab_key + '_period'
    options_widget_KEY  = tab_key + '_D2_options'
    #  for tab 3
    cat_selected_KEY    = tab_key + '_cat_selected'
    low_supply_KEY      = tab_key + '_low_supply'
    #endregion
    
    performance_tabs, target_tabs = st.columns([0.7, 0.3], gap='large')
    
    #region #? LEFT-Tabs
    Staff_1, Dynamic_2, Ledger_3 = (
        performance_tabs.tabs(
            ['Team', 'Store Performance', 'Inventory Report'],
            default     = 'Team',
            key         = tab_key,
            on_change   = 'rerun'
        )
    )
    if Dynamic_2.open:
        with Dynamic_2:
            D2_header, D2_features = st.columns([4, 6], vertical_alignment='bottom')
            with D2_features.container(horizontal=True, horizontal_alignment='right'):
                OPTION_KEY = (
                    st.selectbox("Select Chart",
                        options          = options_list,
                        label_visibility = 'collapsed',
                        format_func      = lambda x: f"{options_list.index(x) + 1}. {x}",
                        key              = options_widget_KEY,
                        width            = 'stretch',
                        )
                    )
                #region Overwrite local_timemode only when selection changes
                if SS.get(history_option_KEY) != OPTION_KEY:
                    SS[history_option_KEY] = OPTION_KEY

                    default_timemode = list(time_label)[1]
                    config_timemode  = DYNAMIC_DATA_OPTIONS.get(OPTION_KEY, {}).get('time', None)
                    SS[local_timemode_KEY] = local_timemode = config_timemode or default_timemode
                #endregion
            
                local_timemode = (
                    st.selectbox(
                    label            = '**Period**',
                    key              = local_timemode_KEY,
                    options          = list(time_label),
                    format_func      = lambda x: time_label[x],
                    width            = 120,
                    label_visibility = 'collapsed')
                )
                dynamic_view = st.radio('View', ['Table', 'Chart'], horizontal=True, label_visibility='collapsed', key=tab_key + '_dynamic_view')
                with st.popover('Logic', icon=':material/code:', width='content', key='show_code_dynamic_ledger', type='tertiary'):
                    st.code(get_source_code(get_dynamic_agg_pivot), language='python', height='stretch')
                    
            with D2_header:
                _head, _, _tail = OPTION_KEY.partition('•')
                d2_main_title = f"{_head.strip()} - {_tail.split('(')[0]}" 
                if '(' in  _tail:
                    d2_main_title = f"{_head.strip()} ({_tail.split('(')[-1]}" 
                styled_header(d2_main_title + ' \u2022', time_label[local_timemode])

    #region #* DATA 1-2
    selected_mask, selected_mode = (
        get_dynamic_mask(
            df_stage_1_date,
            period_anchor,
            period_mode = local_timemode
        )
    )
    #region NOTE: traffic logic update
    _note = """ 
    Vì Join date_traffic từ source nên gây duplicate và sai logic khi `groupby(target, time_mode).sum()`
    - Giải pháp là keep-first-row-of-traffic cho mỗi ngày, fill 0 dòng 2 trở đi.
    - Thực hiện trước khi chạy get_dynamic_dataframe > get_dynamic_dual_agg_pivot.
    - Tạo cột 'store_id' giả làm groupby target, giúp logic groupby(target, time_mode) không lỗi.
    """
    df_double[_STORE] = 'Apple Store'
    drop_traffic_mask = df_double[c.date].duplicated(keep='first')
    df_double.loc[drop_traffic_mask, c.traffic] = 0
    df_double.loc[:, _SUBCAT] = df_double.loc[:, c.subcat].str.replace(r'^Ip', 'iP', regex=True).str.replace('Gb', 'GB', regex=False)
    #endregion
    dynamic_dataframe = get_dynamic_dataframe(
        df_double   = df_double,
        _options    = DYNAMIC_DATA_OPTIONS,
        option_key  = OPTION_KEY,
        _dynamask   = selected_mask,
        period_mode = selected_mode,
        y_limit     = 10
    )
    #endregion #*End
    
    #region #* Tabs 1-2
    if dynamic_dataframe is not None and not dynamic_dataframe.empty:
        units           = DYNAMIC_DATA_OPTIONS[OPTION_KEY].get('units', 'VNĐ')
        unit_key        = units if units in list(TABLE_FORMATTER) else 'qty'
        pivot_bar_dict  = pivot_bar_data(dynamic_dataframe, local_timemode, units)

        if Staff_1.open:
            with Staff_1:
                left_text = 'Staff Performance \u2022'
                left_subtext = time_mode_title
                styled_header(left_text, left_subtext)
                pivot_bar(**pivot_bar_dict)
        
        if Dynamic_2.open:
            with Dynamic_2:
                if dynamic_view == 'Table':
                    st.dataframe(
                        dynamic_dataframe.style.format(
                            TABLE_FORMATTER[unit_key])
                            .highlight_max(axis=0, color='#DDEFFF'),
                    height=380
                    )
                elif dynamic_view == 'Chart':
                    pivot_bar(**pivot_bar_dict)
    else: 
        with Dynamic_2: st.info('No data for selected option.')
    #endregion
    
    #region #* Tab 3
    if Ledger_3.open:
        cat_choices    = sorted(dict_stage_2[c.cat], key=custom_sort)
        compact_ledger = get_compact_stockledger(
            ledger_source     = stock_ledger,
            end_period        = today,
            _date             = s.date,
            sku_and_lot       = [s.sku, s.lot],
            start_end_numeric = [s.start, s.end]
            )

        with Ledger_3:
            led_header, led_options = st.columns([4, 6], vertical_alignment='bottom')
            with led_options.container(horizontal=True, horizontal_alignment='right'):
                if st.selectbox(
                    'Select Category',
                    ['ALL', *cat_choices],
                    index            = 1,
                    key              = cat_selected_KEY,
                    label_visibility = 'collapsed'
                ) == 'ALL':
                    cat_selected = None
                else:
                    cat_selected = SS[cat_selected_KEY]

                show_low_supply = st.selectbox(
                    'score_mode',
                    ['ALL', 'Low Supply'],
                    index            = 1,
                    label_visibility = 'collapsed',
                    key              = low_supply_KEY
                    )
                ledger_view = st.radio(
                    label            = 'Ledger_View',
                    options          = ['Table', 'Chart'],
                    horizontal       = True,
                    label_visibility = 'collapsed',
                    key              = tab_key + '_ledger_view'
                    )
                
                with st.popover('Logic', icon=':material/code:', width='content', key='show_code_score_ledger', type='tertiary'):
                    st.code(get_source_code(get_compact_stockledger), language='python', height='stretch')
                    st.code(get_source_code(category_stock_status), language='python', height='stretch')
            with led_header:
                led_main_title = 'Inventory' if ledger_view == 'Table' else 'Ledger Scatter'
                styled_header(led_main_title + ' \u2022', cat_selected or 'All SKU')
            
            #* Show Table & Chart
            scored_ledger = category_stock_status(compact_ledger, cat_selected, show_low_supply == 'Low Supply')
            if ledger_view == 'Table':
                interact_Combo(
                    stock_ledger    = stock_ledger,
                    scored_ledger   = scored_ledger,
                    start_period    = period_anchor,
                    end_period      = today,
                    columns_config  = columns_config,
                    date_col        = s.date,
                    interact_col    = s.sku,
                    highlight_col   = s.end,
                    prod_name_col   = s.prod_name,
                    table_key       = tab_key + '_interact_Ledger',
                    table_height    = 410
                )
            else:
                ledger_scatter_chart(ledger_scatter_data(scored_ledger))
    #endregion
    
    #endregion LEFT-Tabs

    #region #? RIGHT-Tabs
    progress, achievement = target_tabs.tabs(
        ['KPI Progress', 'Target Achievement'],
        default='KPI Progress',
        key='kpi_tabs',
        on_change='rerun'
        )
    if progress.open:
        with progress:
            styled_header('KPI Progress')
            chart_config = get_month_progress(rounded_month_df, df_target, month_year, c.cat, c.revenue)
            horizon_bar_chart(chart_config, height=360)

    if achievement.open:
        with achievement:
            styled_header('Target Achievement •', time_mode_title)
            hyper_bar_chart(*get_revenue_vs_target_data(stage_1_dynamic, df_target, period_mode, _join_on=c.month), height=400, chart_id='Rev vs. Target')
    #endregion

