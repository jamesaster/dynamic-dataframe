import streamlit as st
from pathlib import Path
from functools import partial
from src.stockledger import *
from visuals.visuals_helper import *
from sections.dashboard.get_data import *
from views.test import sales_pipeline_demo

#region #? Path
BASE_DIR = Path(__file__).parent
path_dummy_ledger   = BASE_DIR / 'stock_ledger_dummy.csv'
price_cat_ledger    = BASE_DIR / 'price_cat_ledger.csv'
@st.cache_data
def dummy_pandas(path):
    file_name = path.name if hasattr(path, 'name') else str(path)
    ext = file_name.split('.')[-1].lower()

    if ext in ['xlsx', 'xls']:
        df = pd.read_excel(path)
    elif ext == 'csv':
        df = pd.read_csv(path)
    elif ext == 'parquet':
        df = pd.read_parquet(path)
    else:
        return

    return df.drop(columns='Unnamed: 0', errors='ignore').convert_dtypes()
@st.dialog('Source Code', width='large')
def code_dialog(func):
    return st.code(get_source_code_raw(func))
def get_memory_usage(df):
    if isinstance(df, pd.Series):
        size_in_bytes = df.memory_usage()
    else:
        size_in_bytes = df.memory_usage().sum()
    if size_in_bytes < (1024 * 1024):
        return f"{size_in_bytes / 1024:.1f} KB"
    else:
        return f"{size_in_bytes / (1024**2):.2f} MB"
#endregion

data = load_files_from_drive()
raw_ledger    = data.get('DEMO_stock_ledger_dummy.csv', pd.DataFrame())
addition_info = data.get('DEMO_price_cat_ledger.csv', pd.DataFrame())
demo_anonym   = data.get('DEMO_Anonym_Price.csv', pd.DataFrame())
demo_sales    = data.get('DEMO_sales_dummy_1000.csv', pd.DataFrame())
demo_traffic  = data.get('DEMO_TRAFFIC.parquet', pd.DataFrame())


OPTION_PIPES   = ['Stock Ledger', 'Sales Data']
OPTIONS_LEDGER = {
    'Process Stock Ledger (Chuẩn Hóa Stock Ledger)': {
        'description': (
            '- **Cân chỉnh cấu trúc hàng:** Dùng kỹ thuật dịch chuyển mảng (`buffer-shifting`) để sửa các dòng dữ liệu thô bị xô lệch cột.\n\n'
            '- **Khôi phục cột thời gian:** Tự động trích xuất và thực hiện (`forward-fill`) thời gian cho các dòng bị thiếu thông tin.\n\n'
            '- **Lọc nhiễu hệ thống:** Triệt tiêu hoàn toàn các dòng trống và dòng tổng hợp tự động do phần mềm sinh ra.\n\n'
            '- **Chuẩn hóa mã sản phẩm:** Sử dụng Regex để bóc tách chuỗi tổ hợp thành mã SKU và tên sản phẩm sạch.\n\n'
            '- **Tối ưu hóa dung lượng:** Thực hiện (`downcast`) các kiểu dữ liệu số để giảm thiểu tiêu thụ RAM.'
        ),
        'func': process_stockLedger
    },
    'Calculate Inventory Value (Tổng giá trị tồn kho)': {
        'description': (
            '- **Dựng ma trận chuỗi thời gian 2D:** Biến đổi dữ liệu kho với dòng (Index) là cặp SKU + IMEI/SN và cột (Columns) là chuỗi ngày liên tục.\n\n'
            '- **Bảo toàn điểm neo (Anchor):** Giữ lại mọi điểm số lượng biến động (kể cả mốc bằng 0) để chống sai lệch lịch sử dòng chảy hàng hóa.\n\n'
            '- **Xử lý đứt gãy lịch sử:** Thực hiện `ffill` theo chiều ngang (trục thời gian) để lấp đầy số tồn cho những ngày không phát sinh giao dịch.\n\n'
            '- **Gộp mảng và định giá lũy kế:** Thu gọn ma trận về cấp độ SKU để tính tổng số lượng, sau đó tích hợp đơn giá để kết xuất tổng giá trị tồn kho theo thời gian.'
        ),
        'func': get_inventory_value
    },
    'Inventory Report As Of (Báo cáo tồn kho chi tiết)': {
        'description': (
            '- **Định danh chi tiết SKU-IMEI/SN:** Kết xuất dữ liệu kho ở độ phân giải cao nhất, mỗi cặp mã SKU kèm số IMEI/Serial Number sẽ hiển thị rõ trạng thái tồn kho và ngày giao dịch cuối cùng.\n\n'
            '- **Khớp lịch sử bất đồng bộ:** Dùng thuật toán `pd.merge_asof` (cơ chế `backward`) để quét ngược chuỗi thời gian và bắt đúng bản ghi giao dịch gần nhất trước mốc `As-Of Date`.'
        ),
        'func': get_inventory_as_of
    },
    'Specific Inventory As Of (Biến động và tồn cuối theo danh mục)': {
        'description': (
            '- **Truy vấn linh hoạt theo cấp độ:** Cho phép cô lập dữ liệu theo từng tầng phân cấp vận hành tùy chọn: Toàn ngành hàng (`Cat`), Nhóm hàng (`Subcat`) hoặc một mã sản phẩm cụ thể (`SKU`).\n\n'
            '- **Tự động bù ngày không phát sinh:** Điền khuyết các ngày trống để tạo ra chuỗi thời gian liên tục, đảm bảo tổng hợp đầy đủ mọi metric tại từng thời điểm.\n\n'
            '- **Xử lý đặc tính dữ liệu cấu trúc phẳng:** Nhận diện thuộc tính phân mảnh của Serial Number, ngăn chặn hành vi cộng dồn (groupby sum) sai lệch tại các điểm tồn đầu và tồn cuối.\n\n'
            '- **Thuật toán tính tồn cuối lũy kế:** Loại bỏ dữ liệu đầu cuối thô, thực hiện tính toán tồn cuối dựa trên tổng tích lũy (`Cumulative Sum`) của cột biến động, chạy từ thời điểm xuất hiện sớm nhất của SKU thuộc cấp độ tương ứng.'
        ),
        'func': get_specific_inventory_as_of
    },
    'Stock Ledger As Of (Báo cáo biến động kho tích hợp số tồn)': {
        'description': (
            '- **Ép dẹt và đồng bộ lịch sử:** Gộp toàn bộ luồng dữ liệu biến động quá khứ và hiện tại thành một bảng phẳng đầy đủ, bao quát toàn bộ vòng đời sản phẩm.\n\n'
            '- **Áp số tồn đầu theo SKU:** Tự động xác định và điền khuyết số tồn đầu kỳ cho từng SKU riêng biệt, tạo điểm neo chính xác để đối chiếu dữ liệu.\n\n'
            '- **Bảo toàn dòng chảy hàng hóa:** Triệt tiêu hoàn toàn hiện tượng đứt gãy dữ liệu thời gian, đảm bảo việc theo dõi lịch sử nhập xuất tại cửa hàng luôn nhất quán.'
        ),
        'func': get_stockledger_as_of
    },
    'Compact Stock Ledger (Bảng tồn kho phẳng từ dữ liệu phân mảnh)': {
        'description': (
            '- **Nén dữ liệu:** Nén toàn bộ lịch sử biến động phức tạp về bảng phẳng hiển thị 1 SKU/dòng.\n\n'
            '- **Tái phân bổ:** Gom các cột điều chỉnh kho + kiểm kê, phân loại âm dương và gộp với tổng Nhập/Xuất.\n\n'
            '- **Tính tuổi hàng hóa:** Tính khoảng cách ngày từ thời điểm tra cứu về các mốc Nhập/Bán/Xuất-Đầu/Cuối để xác định tuổi hàng (`Age Parameters`).\n\n'
            '- **Tính vận tốc bán hàng:** Tính lượng hàng bán ra trong 60 ngày gần nhất để làm cơ sở tính vận tốc bán hàng cho mỗi SKU.'
        ),
        'func': get_compact_stockledger
    },
    'Category Stock Status (Phân loại SKU, chấm điểm Cung - Cầu)': {
        'description': (
            '- **Mô hình ma trận Cung - Cầu:** Áp dụng thuật toán chấm điểm kép (`Scoring Matrix`) để tự động phân loại trạng thái sức khỏe của từng SKU.\n\n'
            '- **Phân tích tốc độ bán (Velocity):** Kết hợp trọng số bán vòng đời (30%) và 60 ngày gần nhất (70%) để tính số ngày dự trữ sản phẩm (`Days of Cover`).\n\n'
            '- **Áp dụng phân vị động:** Đối với thiết bị dựa trên rank và `pd.qcut`, kết hợp `hard_floor` riêng cho phụ kiện.\n\n'
            '- **Định vị Supply theo ngành hàng:** Dùng `pd.cut` chia trạng thái từ (`Low`) tới (`Overstock`). Biên độ phân bổ tự động co giãn theo nhóm sản phẩm.\n\n'
            '- **Bộ lọc nhanh (`show_low_supply`):** Tự động lọc những SKU đang có sức mua cao (Demand 3-4) nhưng lượng tồn hết hoặc chạm ngưỡng báo động (Supply 3-4) làm cơ sở bù hàng khẩn cấp.'
        ),
        'func': category_stock_status
    }
}
recolumns      = [
    'date', 'lot', 'opening', 'audit', 'transfer', 'issued', 'received', 
    'sales', 'returns', 'receipts', 'refunds', 'transit', 'buyback', 
    'icc', 'reversal', 'closing', 'remarks', 'status', 'metadata'
]
with st.sidebar:
    st.header(':material/schema: **Pipeline Selection**')
    pipeline_choice = st.radio(
        '**Select Data Pipeline**',
        options = OPTION_PIPES,
        index   = 0,
        key     = 'demo_pipeline_selector'
    )

    st.divider()
    st.markdown(
        '''
        <div style='
            text-align: left; 
            padding: 0 10px;
            color: rgba(255, 255, 255, 0.4); 
            font-size: 13px; 
            line-height: 1.6;
        '>
            <span class='material-symbols-outlined' style='
                font-size: 16px; 
                vertical-align: text-bottom; 
                margin-right: 5px; 
                color: rgba(255, 255, 255, 0.5);
            '>lock</span><b>PRIVACY & SECURITY</b><br>
            All system data has been encrypted and anonymized to safeguard business confidentiality and privacy.
            <br><br>
            Operational trends and price dynamics are fully preserved using structure-preserving noise algorithms.
        </div>
        ''',
        unsafe_allow_html=True
    )

#region #? Display


title_suffix = 'Functions' if pipeline_choice == 'Stock Ledger' else 'Pipeline'
st.title(f':material/list: {pipeline_choice} {title_suffix}')

if pipeline_choice == 'Stock Ledger':
    st.selectbox(
        label            = 'Select Function',
        options          = list(OPTIONS_LEDGER),
        key              = 'LG_selected_option',
        width            = 600,
        format_func      = lambda x: f'{list(OPTIONS_LEDGER).index(x) + 1}. {x}',
        label_visibility = 'collapsed',
    )
    show_key: str      = st.session_state.get('LG_selected_option', list(OPTIONS_LEDGER)[0])
    height             = 360
    columns_ratio      = [1, 4]
    start_period       = pd.to_datetime('08-05-2029', dayfirst=True)
    end_period         = pd.to_datetime('18-05-2029', dayfirst=True)
    raw_ledger.columns = recolumns
    cleaned_ledger     = process_stockLedger(raw_ledger)
    cleaned_ledger.insert(2, s.cat, addition_info[s.cat].values)
    cleaned_ledger[s.price] = addition_info[s.price].values
    tree_event         = {s.cat: 'APPLE ACC'}
    column_config      ={
        'date': st.column_config.DatetimeColumn('Date', format='DD/MM/YYYY')
    }

    # Catching Function
    if show_key == list(OPTIONS_LEDGER)[0]:
        final_ledger = cleaned_ledger
        function = process_stockLedger
    else:
        function = OPTIONS_LEDGER[show_key]['func']
        try:
            final_ledger = function(cleaned_ledger)
        except:
            try:
                final_ledger = function(cleaned_ledger, AS_OF_DATE=end_period)
            except:
                try:
                    final_ledger = function(cleaned_ledger, start_period=start_period, end_period=end_period, tree_event=tree_event)
                except:
                    try:
                        final_ledger = function(cleaned_ledger, start_period=start_period, end_period=end_period)
                    except:
                        try:
                            final_ledger = function(cleaned_ledger, end_period=end_period)
                        except:
                            compact_ledger = get_compact_stockledger(cleaned_ledger, end_period)
                            final_ledger = category_stock_status(compact_ledger)

    st.space()
    One, Two    = st.columns(columns_ratio, gap='large')
    Three, Four = st.columns(columns_ratio, gap='large')
    with One:
        styled_header('Execution Metrics')
        raw_memory_usage = get_memory_usage(raw_ledger)
        pro_memory_usage = get_memory_usage(final_ledger)

        st.container(border=True, height=height).write(
            f"""
            :material/data_check:
            - **Raw Data Length:** {len(raw_ledger):,} rows\n
            - **Raw NaN Count:** {raw_ledger.isna().sum().sum()} cells\n
            - **Raw Memory Usage:** {raw_memory_usage}\n
            ---------------------------------------------
            - **Result Data Length:** {len(final_ledger):,} rows\n
            - **Result NaN Count:** {final_ledger.isna().sum().sum()} cells\n
            - **Result Memory Usage:** {pro_memory_usage}\n
            """)

    with Two:
        styled_header('Raw Stock Ledger')
        st.dataframe(raw_ledger, height=height)
        st.space()

    with Three:
        styled_header('Function Description')
        st.info(OPTIONS_LEDGER[show_key]['description'])
        if st.button('**View Source Code**', width='stretch', type='secondary', icon=':material/code:'):
            code_dialog(function)

    with Four:
        pre_four, suf_four = show_key.replace(')', '').split(' (')
        styled_header(pre_four + '\u2000-', suf_four)
        if isinstance(final_ledger, pd.Series):
            final_ledger = final_ledger.reset_index().rename(columns={'index': 'date', 0: 'Total Value'})
            final_ledger['Total Value'] = final_ledger['Total Value'].map(format_number)
        st.dataframe(final_ledger, column_config = column_config, height = height + 100)
        st.caption('Một số vị trí hiển thị None do dữ liệu mẫu không có sẵn lịch sử nhập hàng gốc làm mốc đối chiếu.')

if pipeline_choice == 'Sales Data':
    sales_pipeline_demo(
        raw_df  = demo_sales,
        anonym  = demo_anonym,
        traffic = demo_traffic
    )



#endregion