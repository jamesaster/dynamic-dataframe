import streamlit as st
import pandas as pd
import numpy as np

# xác định cột thừa:
drop_cols = ['ins_fee','disc_percent','disc_amount','vat']
# Xác định cột clean manual:
special_cols = ['date', 'time', 'qty', 'ins_stt']

def clean_data(df, drop_cols, special_cols):
    # Xóa cột thừa:
    df_dropped = df.drop(drop_cols, axis=1, errors='ignore')

    # Xác định number & string Columns:
    num_cols = df_dropped.select_dtypes(include='number').columns.to_list()
    str_cols = df_dropped.select_dtypes(include=['string', 'object']).columns.to_list()

    # Tách biệt special cols khỏi number & string để xử lý riêng:
    number = list(set(num_cols) - set(special_cols))
    string = [col for col in str_cols if col not in special_cols]

    # Tạo rules để fillna:
    fill_rules = {
        **dict.fromkeys(number,   0   ),      # dict.fromkeys([list_of_key], allow_single_value) 
        **dict.fromkeys(string,'Unknown'),    # Nhiều keys but return 1 value or the same list.
        'qty': 0,
        'ins_stt': False }
    
    #! lambda là anh em sinh đôi của def nên viết kiểu (lambda x: )
    # lambda x: x.something + 19 ... (Nó là cái cổng chuyển đổi, vứt 1 cái vào thì chạy 1 lần)    
    # apply() = Arrayformula, loop...(Thằng này tự động vứt đồ vào lambda)
    df_clean = (df_dropped
        .replace(['', 'N/A', 'na', 'n/a', 'NaN'], np.nan)
        .apply(lambda c: c.str.strip() if c.dtype == 'object' else c)   # =IF + for loop (as list comprehension)
        .fillna(value=fill_rules))
    
    #! Astype
    types_map = {
        'invoice': 'int64',             # Nếu astype('int64') cho col có NaN, Pandas sẽ lỗi. Nên dùng Int64, Int32 để có thể chứa NaN 
        'ean'    : 'string',            # Nhưng pipeline đã xử lý ở đoạn replace và fillna rồi nên k lo
        'qty'    : 'int32',
        'cat'    : 'category' }         # Lưu ý nếu có thêm cat mới cần df['cat'].cat.add_categories(['New_thing'])
    df_clean = df_clean.astype(types_map, errors='ignore')

    return df_clean

# thêm xử lý qty, price, bị âm hoặc có vđ

clean_data()


@st.cache_data
def load_clean(file_path):
    raw = pd.read_csv(file_path)
    cleaned_df = clean_data(raw)
    return cleaned_df

df = load_clean('D:/Python/LMHN_2025.csv')








# st.set_page_config(
#     page_title='Dynamic Dataframe',
#     page_icon='🐍',
#     layout='wide',
#     initial_sidebar_state='auto', #"auto", "expanded", or "collapsed"
#     menu_items={'About': '2'})


# st.sidebar.title('Options')
# st.sidebar.header('Meo meo')
# che_do = st.sidebar.radio("Chế độ hiển thị:", ["Đầy đủ", "Tóm tắt"])
# st.sidebar.write(f"Bạn đang chọn chế độ: {che_do}")


# st.title('Dynamic Dataframe')
# st.header('About the project')
# st.subheader('I transform data frames into living entities🌿')


# left, right = st.columns([1, 2])
# with left:
#     st.subheader("Nhập liệu")
#     ten_sinh_vat = st.text_input("Tên của DataFrame:")
#     loai = st.selectbox("Hệ:", ["Số học", "Văn bản", "Thời gian"])
# with right:
#     st.subheader("Trạng thái")
#     st.write(f"Đang khởi tạo sinh vật: **{ten_sinh_vat}**")
#     st.write(f"Thuộc hệ: **{loai}**")










# streamlit run "D:\Python\Dynamic_Dataframe\dynamic-dataframe.py"