import pandas as pd
import numpy as np
from src import fill_origin_price, stage_0, rev_validate

def mimic_price_history_and_payments(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    ## Stage 7 - Pricing history mimicking & Restructuring payment methods
    Tái cấu trúc logic tài chính: Mô phỏng lịch sử biến động giá và chuẩn hóa phương thức thanh toán.

    Quy trình thực hiện:
    1. Khôi phục Giá gốc (Price Mimicking): 
       - Tính toán giá gốc từ Doanh thu, Chiết khấu và Số lượng.
       - Tạo 'price_ratio' để ghi nhận tỷ lệ biến động giá thực tế của từng SKU trong lịch sử.
    2. Áp dụng Xu hướng giá ẩn danh:
       - Nhân giá ẩn danh mới (new_price) với 'price_ratio' để mô phỏng lại các đợt giảm giá/khuyến mãi 
         nhưng trên nền giá đã được thay đổi.
       - Làm tròn giá về các mốc thương mại (ví dụ: ...90,000đ) để dữ liệu trông tự nhiên.
    3. Tái cấu trúc Thanh toán (Payment Restructuring):
       - Tính tỷ lệ đóng góp của từng phương thức thanh toán gốc trên doanh thu.
       - Gom nhóm 7 phương thức thanh toán cũ thành 3 nhóm chính: Tiền mặt (Cash), Thẻ (Card), và QR Code.
       - Xử lý các sai số làm tròn để đảm bảo tổng các phương thức bằng 100%.
    4. Tính toán lại Doanh thu (Revenue Re-computation):
       - Xóa cột doanh thu gốc và sử dụng hàm 'rev_validate' để tính toán doanh thu mới dựa trên 
         giá ẩn danh đã mô phỏng và các cột chiết khấu.
       - Phân bổ lại số tiền tuyệt đối cho 3 nhóm thanh toán mới dựa trên doanh thu mới này.
    5. Kiểm soát chất lượng (Validation):
       - Kiểm tra sự khớp nhau giữa Tổng thanh toán và Doanh thu.
       - Gán NaN cho các dòng lỗi logic (ví dụ: có doanh thu nhưng không có phương thức thanh toán).
    """
    # NOTE: Extracting originals product prices from revenue, filling missing price.
    # NOTE: Extracting pricing ratios per SKU throughout history.
    # NOTE: Re applying ratios into anonymized price (preserving data integrity)
    # NOTE: Compute payment ratios then grouping into 3 groups

    df = fill_origin_price(df, price_ratio=True)

    # --------------- Apply price trend/ Replace original price col ---------------
    if 'new_price' in df.columns:  
        df['test_price'] = df['new_price'].astype(float)
        p_ratio_mask = (df['price_ratio'] != 1) & df['price_ratio'].notna()
        df.loc[p_ratio_mask, 'test_price'] = (
            np.floor(
                (df.loc[p_ratio_mask, 'test_price'] * df.loc[p_ratio_mask, 'price_ratio']) / 200_000
                ) * 200_000 + 90_000
            )
        df['price'] = df['test_price']

    # HACK Ensure that only a single column including 'price' is retained to trigger rev_validate()
    to_clean = ['new_price', 'phone', 'price_ratio', 'test_price'] 
    df = df.drop(to_clean, axis=1, errors='ignore') 

    # ----- Compute payment ratio for 7 methods then grouping into 3 methods ------
    if 'vnpay' in df.columns:
        raw_payment_ratios = (
            df[config['payment_cols']]
            .div(df['revenue'].mask(df['revenue'] == 0), axis=0)
            .fillna(0)
        )
        # Create compact payment_ratios ['cash', 'card', 'qr_code']
        payment_ratios = pd.DataFrame(0, columns=['cash'], dtype=np.float64, index=df.index)
        # Grouping & assign payment methods
        payment_ratios['cash'] = raw_payment_ratios['cash']
        payment_ratios = payment_ratios.assign(
            card = raw_payment_ratios[['card', 'payoo', 'banking']].sum(axis=1),
            qr_code = raw_payment_ratios[['mkt', 'vnpay', 'trade_in']].sum(axis=1)
        )

        payment_ratios['count'] = (payment_ratios[['cash', 'card', 'qr_code']] != 0).sum(axis=1)


        # Create mask for rounding invalid ratios
        single = payment_ratios['count'] == 1
        valid_1 = np.isclose(payment_ratios[['cash', 'card', 'qr_code']].sum(axis=1), 1)
        valid_0 = np.isclose(payment_ratios[['cash', 'card', 'qr_code']].sum(axis=1), 0)
        round_mask = single | valid_0 | valid_1

        print(f'DEBUG Final invalid payment ratio: {(~round_mask).sum()}')
        payment_ratios.loc[round_mask] = payment_ratios.loc[round_mask].round()
        # Create no_payment mask
        payment_ratios['no_payment'] = (payment_ratios[['cash', 'card', 'qr_code']] != 0).sum(axis=1) < 1
        df = df.drop(config['payment_cols'], axis=1, errors='ignore')

    #------------------------------------------------------------------------------

    # HACK Drop 'revenue' to trigger recomputation in rev_validate()
    # This ensures consistency between re-created prices and revenue.
    if 'revenue' not in df.columns: print("8. 'revenue' does not exist")
    else:
        df = df.drop('revenue', axis=1, errors='ignore')
        print('# 8. ----- Remove original revenue --')

            # stage_0 catagorizing columns results needed
        _, cat_results, _ = stage_0(df)

            # Revenue recomputating
        print('# 8. ----- Re_computing revenue -----')
        df = rev_validate(
            df, 
            disc_cols=config['disc_cols'], 
            results=cat_results)
        print('# 8. ----- Added revenue col --------')

            # Re-creating compact version of payment methods 
        df = df.assign(
            cash = df['revenue'] * payment_ratios['cash'],
            card = df['revenue'] * payment_ratios['card'],
            qr_code = df['revenue'] * payment_ratios['qr_code'],
            no_payment = payment_ratios['no_payment']
        )

            # Mask for final step revenue validation
        payment_error_mask = (
            ~np.isclose(df[['cash', 'card', 'qr_code']].sum(axis=1), df['revenue'])       #  revenue != payment.sum()
            | (df['no_payment'])                                                          #  payment_method == 0
        ) & (df['qty'] == 0)                                                              #  qty == 0

            # Fill error rows with NaN
        df.loc[payment_error_mask, ['qty', 'revenue']] = np.nan

    return df