import streamlit as st
import pandas as pd
import numpy as np
import inspect
import duckdb
import re
from src import *
from visuals import *
from src.columns import colName as c, colFormat as f, stockCol as s
from sections.dashboard import *
get_drive_trigger() # -> Craete & Update SS.trigger_time
is_james = st.query_params.get('authorize') == st.secrets.env.start_lab
trigger  = SS.trigger_time
time_str, date_str = (trigger.split(maxsplit=1) + [None])[:2]

#region #? 0.   SOURCE

#region (local)
_importance = """
Việc gom tất cả các hàm read_file vào cùng 1 @st.cache_data 
và sử dụng duckdb để (read & filter & join), pandas để (insert cột)
giúp tối ưu thời gian load web nhanh gấp rất nhiều lần
"""
# Path
from pathlib import Path
BASE_DIR = Path(__file__).parent
path_parquet        = BASE_DIR / 'data_output' / 'Run_Forest_Run.parquet'
path_raw_ledger     = BASE_DIR / 'CSV_read_only' / 'stockledgerreportforjames.xls'
path_ledger         = BASE_DIR / 'CSV_read_only' / 'df_ledger_start_to_2026.parquet'
path_hashed_product = BASE_DIR / 'data_output' / 'Anonym_Price_update_sku_include.csv'

#* Raw Sales (save file)
# run_forest_run().pipe(prepare_visualize).to_parquet(
#     path_parquet,
#     engine='pyarrow',       # Apache Arrow tối ưu tốc độ
#     compression='snappy',
#     use_dictionary=True,    # Tối ưu hóa bộ nhớ cho các cột dạng chuỗi/lặp lại
#     index=False
# )

#! Stock Ledger (save file)
# df_ledger = process_stockLedger(path_raw_ledger)
# product_map = stockLedger_hashed_sku_map(df_ledger, r'data_output\Anonym_Price_update_sku_include.csv')
# df_ledger['master_sku'] = df_ledger['sku'].map(product_map)
# # Saved
# df_ledger.to_parquet(path_ledger, compression='snappy')


#? Get final data (Offline)
# stock_ledger, df, min_date, max_date = get_local_data(
#     path_raw=path_parquet,
#     path_saved_ledger=path_ledger,
#     path_product_master=path_hashed_product
# )
#endregion (local)

#region (Google API)
auth_raw  = load_files_from_drive(trigger)
sales_raw = auth_raw.get(authID.sales_name)
stock_raw = auth_raw.get(authID.ledger_name)

app_data  = app_data_bundle(sales_raw = sales_raw, stock_raw = stock_raw)
auth_sales = app_data[1] if app_data else None
SS.analysis_data = auth_sales

demo_data = demo_data_bundle()
stock_ledger, sales_data, min_date, max_date = app_data if is_james else demo_data
if sales_data is None or sales_data.empty:
    if st.button('Reload'):
        load_files_from_drive.clear()
        load_files_from_drive.clear(SS.get('trigger_time'))
    else:
        st.stop()
if is_james:
    current_ts   = (
        f"<strong>{'Today' if sales_data[c.date].iloc[-1].normalize() == today_hanoi() else 'on ' + date_str}</strong>"
        f" at <strong>{time_str}</strong>"
    )
    dash_title = st.secrets.env.store
    dash_sub   = f"""
    <div style="font-size: 0.85rem; color: #6880AA; line-height: 1.5; margin-bottom: 20px; margin-top: 0px;">
        Last transaction updated {current_ts}
    </div>
    """
else:
    dash_title = 'Key Performance Indicators'
    dash_sub   = """
        <div style="font-size: 0.85rem; color: #6880AA; line-height: 1.5; margin-bottom: 20px; margin-top: 0px;">
            This is a <strong>retail operations dashboard</strong>, built for high-fidelity insights.<br>
            Data is synthesized from real-world patterns, ensuring the business logic remains strictly authentic.
        </div>
        """
#endregion (Google API)

#region #* Upload Ledger
with st.sidebar:
    upload_stockLedger(is_james, stock_raw)
#endregion

#endregion #? END

#region Dataframe explained
    #! df 
        # (DataFrame source)
    #! df_stage_1 
        # (Dữ liệu sau khi lọc theo period và nhân đôi range)
    #! period_double 
        # (Dữ liệu applied FILTERS Staff, Cat...) 2x range (double)
    #! period_regular 
        # (Dữ liệu applied FILTERS Staff, Cat...) 1x range (hiện tại)
    #! df_traffic_frozen (fix_traffic)
        # (Dữ liệu traffic NO APPLIED ADVANCE FILTER) - 2x range (double)

    #* df_dynamic 
        # Range dựa trên period mode được select - bảo toàn shape cho WEEK / MONTH
        # Source = (df_stage_1 NO APPLIED ADVANCE FILTER) - 2x range (double)
    #* df_target 
        # (Dữ liệu Revenue Target tính theo ngày/tuần/tháng)
        # Source = (df_stage_1 NO APPLIED ADVANCE FILTER) - 2x range (double)
        
    #? df_inv_masked
        # (Dữ liệu Aggregate theo Invoice Number) Source = period_double
    #? qty_distrib
        # Source = df_invoice + applied dynamic_mask
    #? atv_distrib
        # Source = df_invoice + applied dynamic_mask
#endregion

def main(
    df           : pd.DataFrame,
    stock_ledger : pd.DataFrame,
    min_date     : any,
    max_date     : any,
    dash_title   : str,
    dash_sub     : str
    ):
    """
    # 🐶 Dynamic Dataframe Demo v1.0
    """
    #region  1.   SIDE BAR + FILTER + LOW-LEVEL table
    side_bar_title = ":material/filter_alt: Filters"
    currmonth, prevmonth_days = get_current_past_config(max_date)
    date_config = {
    f'{currmonth} So Far'         : 0,
    f'Past {prevmonth_days} Days' : 1,
        'Past 3 months' : 3, 
        'Past 6 months' : 6, 
        'All time'      : None,
        'Custom'        : 
            {
            'From'      : None,
            'End'       : None 
            }
        }
    
    #region     1a. Sidebar Period Select
    with st.sidebar.container(border=False, height='stretch'):
        date_options = list(date_config.keys())

        if 'period_selected' not in SS:
            SS.period_selected = date_options[0]
        sidebar_date(side_bar_title, date_options, min_date, max_date)
        period = SS.period_selected
    #endregion
    
    #region     1b. df_stage_1 & period_anchor + today
    df_stage_1 = query_df_date(df, date_option=period, date_config=date_config, period_ratio=2)  # Original
    period_anchor: pd.Timestamp = df_stage_1.attrs['period_anchor'].as_unit("ns")
    today: pd.Timestamp = df_stage_1.attrs['today'].as_unit("ns")
    df_stage_1.attrs = {}
    #endregion

    #region     1c. Sidebar Advanced Filters
    filter_cols = ['cat', 'staff', 'invoice']
    dict_stage_2 = get_query_options(df_stage_1.loc[df_stage_1['date'] >= period_anchor], list_col_name=filter_cols)

    with st.sidebar: # Update filter vào side bar
        if 'advanced_selected' not in SS:
            SS.advanced_selected = None
        sidebar_options(dict_stage_2)
        sidebar_signature()
        selected_options = SS.advanced_selected
    #endregion

    #region     1d. Period_double & Period_regular
    period_double = query_df_final(df_stage_1, selected_options).reset_index(drop=True).pipe(clear_attrs)  # Original
    period_regular: pd.DataFrame = period_double.loc[period_double['date'] >= period_anchor]
    #endregion

    #region     1e. Time_mode & 'dynamic_mask' & stage_1_dynamic
    # Dynamic_Mask là 1 function, Apply start và end date vào cột 'date' trong bất cứ df nào có cột date
    period_length: int = (today - period_anchor + pd.Timedelta(days=1)).days
    if 21 <= period_length <= 58:
        view_type = 'Week'
    elif period_length >= 59:
        view_type = 'Month'
    else:
        view_type = 'Day'
    time_mode = GRANULARITY_MAP[view_type]['col']
    time_mode_title = GRANULARITY_MAP[view_type]['title']

    dynamic_mask, period_mode = get_dynamic_mask(df_stage_1, period_anchor, period_mode = time_mode)
    stage_1_dynamic           = df_stage_1.loc[dynamic_mask].reset_index(drop=True)   # phục vụ Revenue vs Target chart
    #endregion

    #endregion

    #region  2.   HIGH-LEVEL table
    fix_traffic      = get_fix_traffic(df_stage_1 = df_stage_1)
    df_target        = get_df_target(df_stage_1 = df_stage_1)
    df_inv_masked    = get_df_invoice(period_double, period_anchor)
    qty_distrib      = get_df_qty_distrib(df_inv_masked)
    atv_distrib      = get_df_atv_distrib(df_inv_masked)
    traffic_distrib  = get_df_traffic_distrib(fix_traffic, period_anchor)
    rounded_month_df = get_round_month_df(
        period_double,
        period_regular,
        period_anchor,
        date_config,
        period
    )
    #endregion

    #region  3.   REFINE CHART CONFIG

    six_months_back = today - pd.DateOffset(months=6)
    show_daily = period_anchor > six_months_back
    show_7d    = period_anchor != six_months_back + pd.Timedelta(days=1)

    daily_condition = 'Daily Revenue' if show_daily else '_Daily Revenue'
    trend_condition = '7D Trend' if show_7d else '_7D Trend'

    rev_n_7dtrend = get_line_pro_data(
        period_double,
        fix_traffic,
        period_anchor,
    ) | {
    'legend_names'  : [daily_condition, trend_condition, 'Traffic'],
    'vlines'        : 
    [{"date": "2024-09-27", "label": "iPhone 16 NPI\n🔥Sep 27, 2024 "},
     {"date": "2025-09-19", "label": "iPhone 17 NPI\n🔥Sep 19, 2025 "}],
    'is_money'      : [True, True, False],
    'main_index'    : 1,
    }
    inventory_value = get_inventory_value(stock_ledger)
    is_hero_inventory = SS.get('Show_Inventory', False)
    if is_hero_inventory:
        # 'Show_Inventory' inside sidebar_options()
        inventory_range = pd.date_range(period_anchor, today)
        y_inventory = inventory_value.reindex(inventory_range, fill_value=0).tolist()
        rev_n_7dtrend['y_lists'][2]      = y_inventory
        rev_n_7dtrend['legend_names'][2] = 'Inventory Value'
        rev_n_7dtrend['is_money'][2]     = True
    
    #endregion

    #region  4.   DISPLAY

    #region #! 1. METRICs
    styled_header(dash_title, h=2)
    st.markdown(dash_sub, unsafe_allow_html=True)

    metric_config = get_metrics_config(
        tf_distrib    = traffic_distrib,
        atv_distrib   = atv_distrib,
        qty_distrib   = qty_distrib,
        period        = period,
        period_anchor = period_anchor,
        today         = today,
        height        = 340
    )
    kpis = get_four_metrics_data(
        period_double = period_double,
        fix_traffic   = fix_traffic,
        period_anchor = period_anchor,
        config_kpi    = metric_config
    )

    four_metrics(kpis = kpis, cols_scale=[1, 1, 1, 1])
    st.space('xxsmall')

    #endregion

    #region #? 2. Hero + Explore [7, 3]
    hero_chart_and_tabs(
        df               = df,
        period_regular   = period_regular,
        period_anchor    = period_anchor,
        today            = today,
        hero_config_raw  = rev_n_7dtrend,
        container_height = 440,
        chart_height     = 400                         
        )
    #endregion

    #region #? 3. Secondary [7, 3]
    target_height = 540
    target_container = st.container(border=False, height=target_height, key='secondary_cont')

    with target_container:
        performance_and_target(
            df_stage_1_date  = df_stage_1[['date']],
            period_double    = period_double,
            stock_ledger     = stock_ledger,
            period_anchor    = period_anchor,
            period_mode      = period_mode,
            today            = today,

            rounded_month_df = rounded_month_df,
            df_target        = df_target,
            stage_1_dynamic  = stage_1_dynamic,

            dict_stage_2     = dict_stage_2,
            global_timemode  = time_mode,
            time_mode_title  = time_mode_title,
            month_year       = c.month,
            tab_key          = 'pfm_tab',
        )
    #endregion
    
    #region #* 4. Treemap - Stock Movement
    HEIGHT_3_4 = 700
    chart_3_4 = st.container(border=False, height=HEIGHT_3_4, gap=None)
    with chart_3_4:
        st.divider() # Phải trong container để không bị cut tooltip
        tree_subtitle = period
        if isinstance(period, dict):
            tree_subtitle = ' ( ' + ' ⟶ '.join(date.strftime('%d %b %y') for _, date in period['Custom'].items()) + ' ) '
        tree_title       = ['Revenue Distribution •', tree_subtitle]
        treemap_data     = get_tree_data(period_regular)
        stock_config     = {
            'ledger_df'     : stock_ledger,
            'start_period'  : period_anchor,
            'end_period'    : today,
            'date_col'      : s.date,
            'first_num_col' : s.start,
            'last_num_col'  : s.end,
            'tree_event'    : None
        }
        treemap_n_stock_movement(
            treemap_data = treemap_data,
            stock_config = stock_config,
            today        = today,
            tree_title   = tree_title,
            chart_id     = 'Happy_Tree_Friend',
            height       = HEIGHT_3_4 - 200,
            col_ratio    = [1, 1.05],
            vertical     = 'top',
            gap          = 'small'
        )
    #endregion

    #endregion -------------------------------------------------------

if __name__ == "__main__":
    main(
        df             = sales_data,
        stock_ledger   = stock_ledger,
        min_date       = min_date,
        max_date       = max_date,
        dash_title     = dash_title,
        dash_sub       = dash_sub
        )
