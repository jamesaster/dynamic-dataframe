import streamlit as st
from src import *
from visuals import *
from src.columns import colName as c
SS = st.session_state
css_hidden_hero = f"<h6 style='text-align: left; color: rgba(52, 67, 109, 0.55); "\
                    "font-weight: 400; font-size: 1rem; letter-spacing: 0px; "\
                    "position: absolute; height: 0; margin-top: -5px; padding: 0px;'>"\
                    "Main series hidden to improve <b style='font-weight: 600;'>Breakdown</b> clarity ✨"\
                    "</h6>"
finder_css_norr = """
    <style>
    [data-testid="stTab"] code {
        font-family: sans-serif !important; 
        letter-spacing: 0.03rem !important;
        font-size: 14px !important;
        font-weight: normal !important;
        color: #333C6C !important;
        background-color: #FFF7DE !important; 
        border-radius: 12px !important;         /* Bo góc */
        padding: 6px 10px !important;           /* Tạo khoảng cách giữa chữ và viền */
        
        white-space: nowrap !important;         /* Không xuống dòng */
        flex-shrink: 0 !important;              /* Không xuống dòng */
    }
    [data-testid="stTab"] code:hover {
        color: #4885FF !important;             
        background-color: #ebf5ff !important;  
        cursor: pointer !important;
    }
    </style>
    """
finder_css_active = """
    <style>
    [data-testid="stTab"] code {
        font: 500 14px/1.5 'Source Sans Pro', sans-serif !important;
        letter-spacing: 0.03rem !important;
        color: #4885FF !important;
        background: #ebf5ff !important;
        border-radius: 12px !important;
        padding: 6px 10px !important;
        white-space: nowrap !important;
        flex-shrink: 0 !important;
    }
    </style>
    """


@st.fragment
def hero_chart_and_tabs(
    *,
    df               : pd.DataFrame,
    period_regular   : pd.DataFrame,
    period_anchor    : pd.Timestamp,
    today            : pd.Timestamp,
    hero_config_raw  : dict,
    container_height : int = 440,
    chart_height     : int = 400
    ):

    is_hero_inventory = SS.get('Show_Inventory', False)
    period            = SS.get('period_selected')
    selected_options  = SS.get('advanced_selected')
    period_length     = (today - period_anchor + pd.Timedelta(days=1)).days

    hero_chart, explore_tabs = st.columns([0.7, 0.3], gap='large')

    # Hero Chart
    with hero_chart:
        hero_title, blue_bar_tip = st.columns([0.7, 0.3])
        is_finder_open = SS.get('four_tabs') == '`🔎 Finder`'

        # is_searching need to update if finder close (avoid delay)
        is_searching = bool(SS.get('Searching')) if is_finder_open else False
        with hero_title:
            main_title = 'Revenue & 7D Trend \u2022 '
            subtitle = period
            if is_finder_open:
                main_title = 'Matched Records \u2022 '
            elif is_hero_inventory:
                main_title = 'Revenue & 7D Trend \u2022 '

            if isinstance(period, dict) and not is_finder_open:
                subtitle = ' ( ' + ' ⟶ '.join(date.strftime('%d %b %y') for _, date in period['Custom'].items()) + ' ) '
            elif is_finder_open and not is_searching:
                subtitle = 'Awaiting Input...'
            elif is_finder_open and is_searching:
                subtitle = f'For\u2000"{SS.get('Searching', '')}"'

            styled_header(main_title, subtitle)
        hidden_note = st.container(border=False).empty()

        hero_state = 'show_all'
        if hero_state: #todo:                                       Complicated hero chart conditions
            is_advanced_filter_on = any(option and option[0] for option in selected_options.values())

            hero_config = hero_config_raw.copy()
            zero_series = [0] * len(hero_config['x_data'])

            #? state switching
            if is_finder_open:
                hero_state = 'hide_all'
            elif is_advanced_filter_on and period_length <= 360: 
                hero_state = 'hide_one'

            #? state applying
            if  hero_state == 'show_all':
                hero_config = SS.get(hero_config, False) or hero_config
                with blue_bar_tip:
                    CSS_custom_blue_tips('Click', 'a blue bar', 'to explore')
            elif hero_state == 'hide_one':
                hero_config['y_lists'][1] = zero_series
                hidden_note.markdown(css_hidden_hero, unsafe_allow_html=True)
            elif hero_state == 'hide_all':
                hero_config['y_lists'] = [zero_series] * 3
                with blue_bar_tip:
                    CSS_custom_blue_tips('Click', 'Invoice ID', 'for details', down=40)

        if not is_searching:
            with st.container(border=False, height=container_height):
                raw_main_event = line_chart_pro(**hero_config, height=chart_height)
                if 'raw_main_event' not in SS or raw_main_event['chart_event']:
                    SS.raw_main_event = raw_main_event
        elif is_finder_open and is_searching:
            results_container = st.container(border=True, height=container_height, vertical_alignment='distribute')

    # Explore
    with explore_tabs:
        if 'Finder_event_flag' not in st.session_state:
            # Bổ sung để không lỗi ngớ ngẩn chưa giải thích đc
            st.session_state['Finder_event_flag'] = False
        event_date = sync_event(
            period_regular,
            c.date,
            today,
            'Finder_event_flag',
            'raw_main_event',
            'Searching_Date'
        )
        chart_2_sub_title = event_date.strftime('%d %B %Y')
        if 'last_event_date' not in SS or SS['last_event_date'] != event_date:
            # ┏─ 🚩 CHƯA CẮM ────────────────┓
            # OR                              ├────► CẮM 🚩 MỚI ──► Hành động
            # ┗─ 🚩 CẮM RỒI nhưng 🚩 BỊ CŨ ──┛
            SS['last_event_date'] = event_date
            st.toast("Explore Refreshed", icon="🔄", duration=1)  
        if not is_finder_open:
            styled_header('Explore \u2022',f'{chart_2_sub_title}')
        else:
            styled_header('Find Anything')


        Finder, L_chart_2, R_chart_2, pie_staff_2 = st.tabs(
            ['`🔎 Finder`', 'Transactions', 'Merchandise', 'Performance'],
            default='Transactions' ,key='four_tabs', on_change='rerun')
        pie_height = 380
        standby_pie = (
            [{'name': '', 'value': 0}], 'pie_key'
            ) # Trường hợp Explore data empty

        if not Finder.open:
            st.markdown(finder_css_norr, unsafe_allow_html=True)
            # Đề phòng, chứ khi chuyển tab widget cũng tự reset
            SS['Searching'] = ''
        
        if Finder.open:
            st.markdown(finder_css_active, unsafe_allow_html=True)
            finder_container = Finder.container(border=False, height=390) # 390px không bị scroll
            with finder_container:
                df_finder, finder_date = finder(df, event_date)
                finder_memory(df_finder, finder_date, 'Searching', limit = 5)
                if is_searching:
                    with results_container:
                        interact_DataFrame(
                            df_finder,
                            df, 
                            finder_date, 
                            c.invoice, 
                            'Finder_Results',
                            invalid_row = None,
                            height = chart_height)

        elif L_chart_2.open:
            with L_chart_2:
                L_interact_col = c.invoice
                L_table_key  = 'left_mini_key'
                daily_invoice_data = get_mini_data(
                    df           = period_regular,
                    event        = event_date,
                    agg_target   = L_interact_col
                )
                if len(daily_invoice_data) > 0:
                    interact_DataFrame(daily_invoice_data, period_regular, event_date, L_interact_col, L_table_key)
                    CSS_custom_blue_tips('Click', 'Invoice ID', 'for details', down=-10)
                else:
                    pie_chart(*standby_pie)

        elif R_chart_2.open:
            with R_chart_2:
                R_interact_col = c.cat
                R_table_key    = 'right_mini_key'
                daily_cat_data = get_mini_data(
                    df           = period_regular,
                    event        = event_date,
                    agg_target   = R_interact_col
                    )
                if len(daily_cat_data) > 0:
                    interact_DataFrame(daily_cat_data, period_regular, event_date, R_interact_col, R_table_key)
                    CSS_custom_blue_tips('Click', 'Category ID', 'for details', down=-10)
                else:
                    pie_chart(*standby_pie)

        elif pie_staff_2.open:
            with pie_staff_2.container(border=False, height=pie_height):
                pie_data = get_pie_data(period_regular, event_date, mode = 'is_event', agg_target = c.staff)
                if not pie_data:
                    pie_data = [{'name': '', 'value': 0}]
                pie_chart(pie_data, 'pie_key', pie_height)
            CSS_custom_blue_tips('Hover', '', 'for details', down=-10, icon='trackpad_input')

        else: # Stanby-mode
            empty_pie = None
            for tab in (L_chart_2, R_chart_2, pie_staff_2):
                if tab.open:
                    with tab:
                        empty_pie = st.empty()
                    break
            if empty_pie is not None: 
                with empty_pie:
                    pie_chart([{'name': '', 'value': 0}], 'pie_key', pie_height)


    # Return for Tree-map subtitle

