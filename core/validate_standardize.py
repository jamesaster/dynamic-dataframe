import pandas as pd
from src.columns import colRaw as r
from src.product_logic import repair_product
from src import *

def smart_data_pipeline (df: pd.DataFrame, config: dict) -> pd.DataFrame:
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
   if not isinstance(df, pd.DataFrame) or df.empty:
      return

   
   #* Prepare for pipeline
   validate_cols = df.columns[:4].tolist()
   na_mask = df[validate_cols].isna().any(axis=1)
   print('Drop NA columns[:4].any():', na_mask.sum(), 'rows')
   sales_df = df.dropna(subset=validate_cols, how='any', ignore_index=True).copy()


   #region #? Categorizing Column
   categorize_results  = stage_1(stage_0(sales_df))
   number_keys         = ['price', 'numeric_col', 'revenue']
   numeric_combine     = sum([categorize_results[group] for group in number_keys], [])
   print('categorize_results', categorize_results)
   # 27-06-26 Update: Vì fetch từ sheet, columns 100% là string nên cần coerce ngay từ bước này
   sales_df[numeric_combine] = sales_df[numeric_combine].apply(pd.to_numeric, axis=0, errors='coerce', downcast='integer')
   #endregion


   if config['prod_info_repair'] == True:
      sales_df = repair_product(sales_df)


   #region #? Validating Revenue | Recovering Date
   sales_df = rev_validate(sales_df, config['payment_cols'], config['disc_cols'], categorize_results)
   sales_df[r.date], _ = recover_date(sales_df, r.date, config['date_anchor'])
   #endregion

   #* After cleaning 
   sales_df = execution(sales_df, categorize_results)

   return sales_df


