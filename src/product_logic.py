from src import stockCol as s, colRaw as r
import pandas as pd
import numpy as np
import streamlit as st
def repair_product(*, sales: pd.DataFrame, stock: pd.DataFrame):
    """
    ## Hàm sửa chữa thông tin cho sản phẩm trong [Sales Data]
    - Cơ chế: dùng lot làm anchor mapping từ stockledger
    - Fallback: manual masking
    """
    print('[repair_product] Activating..')

    #region 1. Normalize Sales lot
    lot_n_imei = sales[r.imei_sn].str.split('/')
    sales[r.imei_sn] = lot_n_imei.str[0]
    #endregion

    #region 2. Product info lookup table form stockledger
    prod_info = stock[stock[s.lot].notna()][[s.lot, s.sku, s.prod_name]]
    prod_info = prod_info.drop_duplicates(subset=s.lot, ignore_index=True)
    prod_info.loc[prod_info[s.lot] == '-', s.lot] = pd.NA
    prod_info = prod_info.dropna(subset=s.lot, ignore_index=True)
    #endregion

    #region 3. Create CAT col from scratch
    cat_map = {
        'phone' : 'IPHONE',
        'ipad'  : 'IPAD',
        'watch' : 'WATCH',
        'mb'    : 'MAC',
        'mac'   : 'MAC'
    }
    conds   = [prod_info[s.prod_name].str.contains(key, case=False, na=False) for key in cat_map]
    choices = [*cat_map.values()]
    prod_info[s.cat] = np.select(conds, choices, 'APPLE ACC')
    #endregion

    #region 4. Mapping
    sales.loc[sales[r.cat] == '-', r.cat] = pd.NA # Update 05/07/26 đảo thứ tự pipeline, thêm bước ép NA
    before_na = sales[r.cat].isna().sum()
    columns   = sales.columns
    error_lot = sales.loc[(sales[r.cat].isna()) & (sales[r.imei_sn] != '-'), r.imei_sn]
    sales     = sales.set_index(r.imei_sn)
    prod_info = prod_info[prod_info[s.lot].isin(error_lot)].set_index(s.lot)
    sales.update(prod_info)
    sales     = sales.reset_index(drop=False)
    #endregion

    print(f'[repair_product] Auto Repaired {before_na - sales[s.cat].isna().sum()} rows')
  
    #region 5. Fallback
    NA_mask    = sales[r.cat].isna()
    is_apple   = sales[r.ean].str.startswith('19', na = False) & NA_mask
    is_3rd_acc = (~is_apple) & NA_mask
    unit_price = sales[r.revenue] / sales[r.qty]
    is_app_acc = (unit_price <= 3_990_000) & is_apple
    is_iphone  = (unit_price >= 20_000_000) & is_apple

    sales.loc[is_app_acc, r.cat] = 'APPLE ACC'
    sales.loc[is_3rd_acc, r.cat] = '3RD ACC'
    sales.loc[is_iphone, r.cat]  = 'IPHONE'
    print(f'[repair_product] Manual fallback -> Apple_ACC = {is_app_acc.sum()} | 3RD_ACC = {is_3rd_acc.sum()} | IPHONE = {is_iphone.sum()}')

    NA_mask = sales[r.cat].isna()
    sales   = sales.dropna(subset=r.cat, ignore_index=True)
    print(f'[repair_product] Removed {NA_mask.sum()} rows')

    NA_mask = sales[r.cat].isna()
    print(f'[repair_product] Error = {NA_mask.sum()}\n')
    #endregion
    
    return sales[columns]

