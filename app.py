import textwrap
import streamlit as st
from visuals.css_inject import *
from sections.dashboard.get_data import *
from core.run_auth_pipe import authentic_pipeline
from src.columns import stockCol as s
import numpy as np
SS = st.session_state
is_james    = SS.get('is_james')
page_title  = 'Dynamic DataFrame'

cached_http_session()

st.set_page_config(
    layout      = 'wide',
    page_title  = page_title,
    page_icon   = '🐶'
    )

# region 0. CSS Inject


st.markdown(SIDE_BAR_CSS, unsafe_allow_html=True)
st.markdown(PAGE_FONT_CSS, unsafe_allow_html=True)
st.markdown(POPOVER_CSS, unsafe_allow_html=True)
st.markdown(MARK_DOWN_CSS, unsafe_allow_html=True)
st.html(DF_SCROLL_HTML)
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
css_hidden_hero = f"<h6 style='text-align: left; color: rgba(52, 67, 109, 0.55); "\
                "font-weight: 400; font-size: 1rem; letter-spacing: 0px; "\
                "position: absolute; height: 0; margin-top: -5px; padding: 0px;'>"\
                "Main series hidden to improve <b style='font-weight: 600;'>Breakdown</b> clarity ✨"\
                "</h6>"
# endregion

# region 1. Get data funcs

# endregion

dashboard_page = st.Page('views/dashboard.py', title='Dashboard', icon=':material/analytics:')
pipelines_page = st.Page('views/demo.py', title='Data Pipelines', icon=':material/rocket_launch:')
shap_page      = st.Page('views/shap_analysis.py', title='Analysis', icon=':material/experiment:')
page_list      = [dashboard_page, shap_page, pipelines_page]
pages          = st.navigation(page_list, position='sidebar', expanded=True)
pages.run()