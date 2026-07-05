import streamlit as st
import pandas as pd
from src import get_specific_inventory_as_of
from visuals.visuals_helper import *
from visuals import treemap_chart, get_stock_movement, tree_event_hyper_chart


@st.fragment
def treemap_n_stock_movement(
    treemap_data: list,
    stock_config: dict,
    today: pd.Timestamp,
    tree_title: list,
    chart_id: str,
    height: int=500,
    col_ratio: list = [1, 1],
    vertical: str = 'top',
    gap: str = 'small'
    ):
    """
    ## TWIN-PANEL INVENTORY DASHBOARD
    ### LAYOUT ARCHITECTURE:
    - **Cột Trái:** Treemap phân tích cơ cấu sản phẩm (kèm Event Click bắt vị trí).
    - **Cột Phải:** Biểu đồ xu hướng kho tương ứng + Hệ thống thẻ chỉ số KPI nhanh (`st.metric`).
    """
    SS = st.session_state
    tree_chart, stock_chart = (
        st.columns(
        spec                = col_ratio,
        vertical_alignment  = vertical,
        gap                 = gap
        )
    )
    
    with tree_chart:
        tree_header, tree_blue_tip = st.columns([0.95, 0.05])
        with tree_header:
            styled_header(tree_title[0], tree_title[1])
        with tree_blue_tip:
            CSS_custom_blue_tips('Click', 'block', 'to focus • 🏠 to return', px=8, down=50)
        st.space(size='xxsmall')
        tree_event = treemap_chart(
            treemap_data = treemap_data,
            chart_id     = chart_id, 
            height       = height
        )
        
        #region #? Event extraction
        if 'tree_inject' not in SS:
            SS.tree_inject = {'cat': 'IPHONE'}
        tree_map = SS.tree_inject

        if tree_event and tree_event.get('chart_event') and tree_event.get('chart_event').get('path') != ['🏠']:
            path = tree_event['chart_event'].get('path', [])[1:]
            tree_level = ['cat', 'detail_sub_lob', 'product_name', 'sku']
            tree_map = {level: name for level, name in zip(tree_level, path)}
        stock_config['tree_event'] = tree_map
        #endregion

    with stock_chart:
        sub_names = list(stock_config['tree_event'].values())
        sub_key   = sub_names[min(2, len(sub_names) - 1)]
        styled_header('Stock Movement •', sub_key)
        st.space(size='xxsmall')

        # Tạo columns phụ cho movement metric
        stock, metric = st.columns([0.81, 0.19], gap=gap)
        metric_height = int(height * 1.1)

        with stock:
            movement_df = get_specific_inventory_as_of(**stock_config)
            stock_movement = get_stock_movement(movement_df)
            print(stock_movement)
            tree_event_hyper_chart(
                chart_data = stock_movement,
                chart_id   = f'stock_movement{chart_id}',
                height     = height         
            )

        with metric.container(border=False, height=metric_height, vertical_alignment='bottom'):
            metric_config = stock_movement["metrics"]
            stock_return  = metric_config['return']

            as_of_date = f"As of {today.strftime('%d-%m-%Y')}"
            for i in range(len(metric_config.get('label', 0))):
                st.metric(
                    label  = metric_config['label'][i],
                    value  = metric_config['value'][i],
                    border = True,
                    height = 'stretch',
                    delta_description = as_of_date if i == 0 else stock_return if i == 2 else None
                    )
