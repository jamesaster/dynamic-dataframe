from datetime import datetime, timedelta
import streamlit as st
import pandas as pd
import numpy as np
import time
import re
from .e_charts import hyper_bar_chart
from src.columns import colName as c
SS = st.session_state
# colNamed


@st.fragment
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
def run_pipe():
    """
    ## Demo pipeline, chưa xong
    """
    if st.button("🚀 Cập nhật dữ liệu ngay"):
        # * hộp trạng thái load pipeline
        with st.status("Đang tải dữ liệu...", expanded=True) as status:
            st.write("Đang kết nối database...")
            time.sleep(1)
            st.write("Đang tính toán KPIs...")
            time.sleep(1)
            status.update(label="Tải dữ liệu xong!",
                            state="complete", expanded=False)
            time.sleep(1)
            st.toast("Đã cập nhật biểu đồ!", icon="🎉")

        st.success("Bạn đã làm rất tốt!", icon="✅")
        # st.warning("Cẩn thận, dữ liệu này có thể bị sai lệch.", icon="⚠️")
        # st.error("Rất tiếc, đã có lỗi xảy ra khi kết nối database.", icon="🚨")
@st.dialog("🎁Chúc mừng")
def _welcome_(context: str, button_text: str):
    # dialog phải đi kèm st.rerun() không là sẽ không tắt đc
    st.write(context)
    if st.button(button_text):
        st.session_state.welcome = 'tạo key welcome'
        st.session_state.balloon = True
        st.rerun()                   # Cái này là reset web, sau nó chả có gì chạy
def _welcome_logic_(context='Bấm tiếp tục để nhận quà.', button_text='Tiếp tục'):
    """
    ### Cái này để làm màu.
    Args:
        context
        button_text
    """
    if 'welcome' not in st.session_state:
        # NOTE -----> Ở đây tạo 2 items 'welcome' và 'balloon' rồi rerun()
        _welcome_(context, button_text)
        # st.session_state chỉ mất khi tắt web, F5.
        # rerun() không ảnh hưởng.
    # Sau khi rerun() ở _welcome_, xuống đây balloon == True và thả bóng, đánh dấu False forever
    if st.session_state.get('balloon', False) == True:
        st.balloons()
        st.session_state.balloon = False


@st.fragment # NOTE side_bar_1()
def sidebar_date(title: str, date_options: list, min_date=None, max_date=None):
    """
    ### Render filter for `Date` filtering only.
    ### Need to wrap it inside with `st.sidebar`:
    Returns:
        any: `period` as a String (pre-defined) or a Dict (Custom range).
    """

    st.title(title)

    with st.container(border=True, width='content'):
        st.caption(f":material/event_available: Available: **{min_date.strftime('%b %Y')}** :material/arrow_right_alt: **{max_date.strftime('%b %Y')}**")
    st.markdown(':material/history: **How far back should we go?**', help='Reference point: Today or the latest available data.')
    
    # 1. Select Period
    period = st.selectbox(
        label='***\\* Period***',
        options=date_options,
        index=0,
        key='period'
    )

    st.markdown('')

    custom_period = {}

    # 2. Logic for 'Custom' option
    if period == 'Custom':
        start_col, end_col = st.columns(2)
        
        with start_col:
            from_date = st.date_input(
                'Start date', value = max_date - pd.DateOffset(days=6), min_value=min_date, max_value=max_date, key='From', format="DD-MM-YYYY",)

        with end_col:
            end_date = st.date_input(
                'End date', value=max_date, min_value=from_date, max_value=max_date, key='End', format="DD-MM-YYYY",)

        st.markdown('---')

        # Pack into dict for Stage 1 compatibility
        custom_period = {'Custom': {'From': from_date, 'End': end_date}}
    
    key_exist = 'period_selected' in SS

    if period != 'Custom':
        if not key_exist or (SS.period != SS.period_selected):
            SS.period_selected = SS.period
            st.rerun(scope='app')
    else:
        if not key_exist or (SS.period_selected != custom_period):
            SS.period_selected = custom_period
            print('RERUN_CUSTOM')
            st.rerun(scope='app')

@st.fragment # NOTE side_bar_2()
def sidebar_options(stage_2_dict: dict = None):
    """
    ### Render advanced filters based on the provided dictionary.
    ### Need to wrap it inside with `st.sidebar`:
    Args:
        stage_2_dict (dict, optional): Dict of col_names: unique_values()

    Returns:
        dict: `selected_options` = Dict of col_names: selected choices.
    """
    selected_options = {}
    if 'advanced_selected' not in SS:
        SS.advanced_selected = {}
    if (not stage_2_dict or not any(stage_2_dict.values())):
        SS.advanced_selected = selected_options

    st.markdown(":material/search: **Advanced Filters**", help='You can pick multiple values.')

    for col, value in stage_2_dict.items():
        col_label = col.replace('_', ' ').title() 
        
        selected_options[col] = (
            st.multiselect(
            label = f"***\\* {col_label}***",
            options = sorted(value, key = custom_sort) if col == c.cat else sorted(value),
            key=f"filter_{col}"
        ))
    
    st.divider()

    with st.popover('Chart Features', icon=':material/settings:', width='stretch', type='secondary'):
        st.write('**Revenue & 7D Trend**')
        but1, but2 = st.columns(2)

        but_keys  = ['c_1_button_a', 'c_1_button_b']
        reset_key = 'Show_Inventory'
        if reset_key not in SS:
            SS[reset_key] = False

        state_1 = SS.get(reset_key)
        state_2 = not SS.get(reset_key)
        label = lambda name: [f'**{name}**', name]
        types = ['secondary', 'tertiary']
        b_1 = but1.button(label('Traffic'  )[state_1], key=but_keys[0], type=types[state_1], width='stretch', help='Show Traffic Count')
        b_2 = but2.button(label('Inventory')[state_2], key=but_keys[1], type=types[state_2], width='stretch', help='Show Total Inventory Value')

        if b_1:
            SS[reset_key] = False
            st.rerun(scope='app')
        elif b_2:
            SS[reset_key] = True
            st.rerun(scope='app')

    #? bẫy chặn double rerun
    if not 'sbar_rerun_counter' in SS:
        SS.sbar_rerun_counter = 0
    if  SS.sbar_rerun_counter <= 0 and (SS.advanced_selected != selected_options):
        SS.sbar_rerun_counter += 1
        SS.advanced_selected = selected_options
        st.rerun(scope='app')
    else:
        SS.sbar_rerun_counter = 0

@st.fragment
def sidebar_signature():
    """
    ### Đi cùng `sidebar_options()`
    """
    st.space('xlarge')
    # st.html("""
    # <style>
    # @keyframes complex-collision {
    #     0%  { transform: translateX(-150px) rotate(-90deg) scale(1.0);
    #         animation-timing-function: linear;}                             /* Phi ra */
    #     10%  { transform: translateX(40px)  rotate(20deg)  scale(1.0)}      /* Crash */
    #     15% { transform: translateX(35px)   scale(0.5, 1.5)}                /* Nhún */
    #     25% { transform: translateX(20px)   scale(1.3, 1.0)}                /* Re-bounce */
    #     30% { transform: translateX(0px)    scale(1.0)}                     /* Back to Normal*/
    #     40% { transform: translateX(0px)    scale(1.2)}                     /* To ra*/
    #     50% { transform: translateX(0px)    scale(1.0, 0.65)}               /* Lấy đà*/  
    #     57% { transform: translateY(-100px) rotate(-330deg) scale(1)}       /* Jump + Flip*/
    #     70% { transform: translateY(-90px)  rotate(0deg) scale(0.85, 1)}    /* Hover*/   
    #     80% { transform: translateX(0px)    rotate(30deg)}                  /* Landing */
    #     90% { transform: translateX(0px)}                                   /* Nghỉ */
    #     100% { transform: translateX(300px) rotate(-90deg) scale(1);
    #         opacity: 1;
    #         animation-timing-function: linear;}
    # }
    # .animated-icon {
    #     display: inline-block;
    #     font-size: 42px;
    #     line-height: 1;
    #     transform-origin: center;
    #     /* Dùng 'linear' cho đoạn lao vào và 'ease-out' cho đoạn nhún để cảm giác vật lý thật nhất */
    #     animation: complex-collision 5s infinite ease-out;
    # }
    # </style>
    # """) 
    # st.html("""
    #     <div id="dynamic-logo-container" style="margin-top: 35px; padding-bottom: 10px;">
    #         <hr style="margin-top: 0; margin-bottom: 25px; border: none; border-top: 1px solid #485261;">
            
    #         <div style="display: flex; align-items: center; justify-content: center; gap: 15px;">
    #             <span class="animated-icon">⚡</span>
                
    #             <div style="display: flex; flex-direction: column; align-items: flex-start;">
    #                 <span style="font-weight: 700; color: #FAFAFA; font-family: 'Source Sans Pro', sans-serif; font-size: 24px; line-height: 1.1;">
    #                     Dynamic
    #                 </span>
    #                 <span style="font-weight: 700; color: #5A94E8; font-family: 'Source Sans Pro', sans-serif; font-size: 24px; line-height: 1.1; margin-left: 25px; margin-top: 2px;">
    #                     DataFrame
    #                 </span>
    #             </div>
    #         </div>
            
    #         <div style="text-align: center; margin-top: 18px;">
    #             <p style="font-size: 11px; color: #808495; font-style: italic; margin: 0; letter-spacing: 0.6px;">
    #                 Crafted by Tran Anh Hieu
    #             </p>
    #         </div>
    #     </div>
    # """)

    st.html("""
    <style>
        @keyframes complex-collision {
        0%  { transform: translateX(-150px) rotate(-90deg) scale(1.0);
            animation-timing-function: linear;}                             /* Phi ra */
        10%  { transform: translateX(28px)  rotate(20deg)  scale(1.0)}      /* Crash */
        15% { transform: translateX(18px)   scale(0.5, 1.5)}                /* Nhún */
        25% { transform: translateX(8px)    scale(1.3, 1.0)}                /* Re-bounce */
        30% { transform: translateX(0px)    scale(1.0)}                     /* Back to Normal*/
        40% { transform: translateX(0px)    scale(1.2)}                     /* To ra*/
        50% { transform: translateX(0px)    scale(1.0, 0.65)}               /* Lấy đà*/  
        55% { transform: translateY(-50px) rotate(-330deg) scale(1)}        /* Jump + Flip*/
        60% { transform: translateY(-40px)  rotate(0deg) scale(0.85, 1)}    /* Hover*/   
        75% { transform: translateX(0px)    rotate(30deg)}                  /* Landing */
        85% { transform: translateX(0px)}                                   /* Nghỉ */
        100% { transform: translateX(300px) rotate(-90deg) scale(1);
            opacity: 1;
            animation-timing-function: linear;}
        }

        @keyframes toggle-icon-1 { 0%, 50% { opacity: 1; } 51%, 100% { opacity: 0; } }
        @keyframes toggle-icon-2 { 0%, 50% { opacity: 0; } 51%, 100% { opacity: 1; } }

        .animated-icon {
            position: absolute; /* Quan trọng: Đè 2 icon lên nhau */
            top: 0; left: 0;
            display: inline-block;
            font-size: 42px;
            line-height: 1;
            transform-origin: center;
            animation: complex-collision 5s infinite ease-out;
        }
        
        /* Gán toggle animation cho từng icon */
        .icon-1 { animation: complex-collision 5s infinite ease-out, toggle-icon-1 10s infinite; }
        .icon-2 { animation: complex-collision 5s infinite ease-out, toggle-icon-2 10s infinite; }
    </style>
    """) 
    st.html("""
    <div id="dynamic-logo-container" style="margin-top: 35px; padding-bottom: 10px;">
        <hr style="margin-top: 0; margin-bottom: 25px; border: none; border-top: 1px solid #485261;">
        
        <div style="display: flex; align-items: center; justify-content: center; gap: 15px;">
            <div style="position: relative; width: 42px; height: 42px; display: flex; align-items: center; justify-content: center;">
                <span class="animated-icon icon-1">⚡</span>
                <span class="animated-icon icon-2">💡</span>
            </div>
            
            <div style="display: flex; flex-direction: column; align-items: flex-start;">
                <span style="font-weight: 700; color: #FAFAFA; font-family: 'Source Sans Pro', sans-serif; font-size: 24px; line-height: 1.1;">
                    Dynamic
                </span>
                <span style="font-weight: 700; color: #5A94E8; font-family: 'Source Sans Pro', sans-serif; font-size: 24px; line-height: 1.1; margin-left: 25px; margin-top: 2px;">
                    DataFrame
                </span>
            </div>
        </div>
        
        <div style="text-align: center; margin-top: 18px;">
            <p style="font-size: 11px; color: #808495; font-style: italic; margin: 0; letter-spacing: 0.6px;">
                Crafted by Tran Anh Hieu
            </p>
        </div>
    </div>
    """)

