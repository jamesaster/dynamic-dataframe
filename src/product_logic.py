import numpy as np
import pandas as pd
from src import stockCol as s, colRaw as r
from sections.dashboard import load_files_from_drive

def repair_product(sales: pd.DataFrame):
    """
    ## Hàm sửa chữa thông tin cho sản phẩm trong [Sales Data]
    - Cơ chế: dùng lot làm anchor mapping từ stockledger
    - Fallback: manual masking
    """
    # Nhặt stockledger từ RAM
    print('[repair_product] Activating..')
    stock: pd.DataFrame = load_files_from_drive()['DASHBOARD_stock_ledger.parquet']

    #region 1. Normalize Sales lot
    lot_n_imei = sales[r.imei_sn].str.split('/')
    sales[r.imei_sn] = lot_n_imei.str[0]
    #endregion

    #region 2. Product info lookup table form stockledger
    prod_info = stock[stock[s.lot].notna()][[s.lot, s.sku, s.prod_name]]
    prod_info = prod_info.drop_duplicates(subset=s.lot, ignore_index=True)
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
    prod_info[s.cat] = np.select(conds, choices, 'ACCESSORIES (APPLE)')
    #endregion

    #region 4. Mapping
    before_na = sales[r.cat].isna().sum()
    columns   = sales.columns

    sales     = sales.set_index(r.imei_sn)
    NA_mask   = sales[r.cat].isna()
    NA_lot    = sales[NA_mask].index.dropna()
    prod_info = prod_info[prod_info[s.lot].isin(NA_lot)].set_index(s.lot)
    sales.update(prod_info)
    sales     = sales.reset_index(drop=False)
    #endregion

    #region LOG
    print(f'[repair_product] Auto Repaired {before_na - sales[s.cat].isna().sum()} rows')
    print(f'[repair_product] Errors remain {sales[s.cat].isna().sum()} rows')
    #endregion

    #region 5. Fallback
    NA_mask    = sales[r.cat].isna()
    is_apple   = sales[r.ean].str.startswith('19', na = False) & NA_mask
    is_3rd_acc = (~is_apple) & NA_mask
    unit_price = sales[r.revenue] / sales[r.qty]
    is_app_acc = (unit_price <= 3_990_000) & is_apple
    is_iphone  = (unit_price >= 20_000_000) & is_apple

    sales.loc[is_app_acc, r.cat] = 'ACCESSORIES (APPLE)'
    sales.loc[is_3rd_acc, r.cat] = '3RD ACC'
    sales.loc[is_iphone, r.cat]  = 'IPHONE'
    print(f'[repair_product] Manual fallback -> Apple_ACC = {is_app_acc.sum()} | 3RD_ACC = {is_3rd_acc.sum()} | IPHONE = {is_iphone.sum()}')

    NA_mask = sales[r.cat].isna()
    sales   = sales.dropna(subset=r.cat, ignore_index=True)
    print(f'[repair_product] Removed {NA_mask.sum()} rows')

    NA_mask = sales[r.cat].isna()
    print(f'[repair_product] Error = {NA_mask.sum()}\n')
    #endregion

    #region 6. Đồng bộ với pipeline
    sales = sales[sales[r.cat] != 'BANK FEE']
    cat_sync_map = {
        'ACCESSORIES (APPLE)': 'APPLE ACC',
        'QOALA'              : 'APPLE ACC',
        'IPHONE 16'          : 'IPHONE',
    }
    sales[r.cat] = sales[r.cat].replace(cat_sync_map)
    sales = sales.reset_index(drop=True)
    #endregion

    return sales[columns]
