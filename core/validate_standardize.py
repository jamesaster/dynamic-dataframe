import pandas as pd
from src import execution, stage_0, stage_1, rev_validate, recover_date

    # NOTE: Automate column classification using regex and sample-based testing.
    # NOTE: Repair corrupted timestamps using anchor-based logic and gap filling.
    # NOTE: Reconstruct missing revenue using cross-column financial validation.

def smart_data_pipeline (df:pd.DataFrame, config:dict) -> pd.DataFrame:
    """
    ## Stage 2 - Data Validation & Standardization
    Tiền xử lý dữ liệu tự động dựa trên phân loại dtype, logic doanh thu và ngày tháng.

    Quy trình xử lý:
    1. Phân loại (Classification): Quét 1000 dòng mẫu kết hợp tên cột để phân bổ 
       các cột vào Dictionary theo kiểu dữ liệu (numeric, object, datetime...).
    2. Xử lý Doanh thu (Revenue): Tìm cột revenue, xác thực giá trị, điền NaN 
       hoặc tính toán tạo mới nếu thiếu.
    3. Xử lý Ngày tháng (Date): Xác định cột ngày, sử dụng anchor_col và thuật toán 
       chunking để kiểm tra sự khớp nhau giữa năm/tháng, từ đó suy diễn ra ngày còn thiếu.
    4. Thực thi (Execution): Duyệt qua Dictionary phân loại từ bước 1 để ép kiểu (astype) 
       và điền khuyết (fillna) đồng loạt cho toàn bộ DataFrame.
    """
    
    categorize_results = stage_1(stage_0(df))

    df = rev_validate(df, config['payment_cols'], config['disc_cols'], categorize_results)

    df[config['date_pocket']], _ = recover_date(df, config['date_pocket'], config['date_anchor'])

    #* After cleaning
    df_new = execution(df, categorize_results).copy()
    
    return df_new