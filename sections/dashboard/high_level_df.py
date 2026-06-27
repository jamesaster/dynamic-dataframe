import pandas as pd
import numpy as np
from src.columns import colName as c, colFormat as f
from visuals.visuals_helper import clear_attrs
from visuals import get_dynamic_mask

def get_fix_traffic(df_stage_1: pd.DataFrame):
    """ 
    ## Base: df_stage_1 | Only-Time-Filtered
    ## Nhớ là df_traffic_frozen luôn double_period 
    # ( Phục vụ Metrics + Rolling)
    """
    require_cols  = [col for col in ['date', 'week', 'month_year', 'date_traffic'] if col in df_stage_1.columns]

    return (
        df_stage_1[require_cols]
        .groupby('date')
        [['week', 'month_year', 'date_traffic']]
        .first()
        .rename({'date_traffic': 'Total Traffic'}, axis=1)
        .pipe(clear_attrs)
    )
    # output > date: ['week', 'month_year', 'Total Traffic']

def get_df_target(df_stage_1: pd.DataFrame):
    df_target = df_stage_1.groupby(c.month, as_index=False).agg({c.revenue: 'sum'})
    rng       = np.random.default_rng(8)
    target_factor     = rng.uniform(0.83, 1.15)
    seasonal_bias_map = {
        i: float(val) if (val == val) else 0.0 
        for i in range(1, 13, 1) 
        for val in [rng.uniform(-0.05, 0.15)]
    }
    seasonal_bias = 1 + pd.to_datetime(df_target[c.month], format=f.month, errors='coerce').dt.month.map(seasonal_bias_map)
    df_target['month_target'] = (
        np.floor(abs(
        df_target['revenue']
        * target_factor
        * seasonal_bias
        ) // 100_000_000) * 100_000_000)
    days_of_target = pd.to_datetime(df_target[c.month], format=f.month, errors='coerce').dt.days_in_month
    df_target['date_target'] = df_target['month_target'] / days_of_target
    df_target['week_target'] = df_target['date_target'] * 7

    return df_target

def get_df_invoice(period_double: pd.DataFrame, period_anchor: pd.Timestamp):
    """ ## Base of get_df_quantity, get_df_atv_distrib"""
    invoice_agg_cols = [c.date, c.week, c.month, c.invoice, c.staff]
    df_invoice = period_double.groupby(invoice_agg_cols, as_index=False, observed=True).agg({
        c.qty       : 'sum',
        c.revenue   : 'sum',
        # c.pay_card  : 'sum',
        # c.pay_qr    : 'sum'
    })
    df_invoice['order_type'] = df_invoice[c.qty].map({1: 'single'}).fillna('combo').mask(df_invoice[c.qty] < 1)
    df_inv_masked = df_invoice.loc[df_invoice['date'] >= period_anchor]

    return df_inv_masked

def get_df_qty_distrib(df_invoice_masked: pd.DataFrame):
    """ ## Quantity Distribution"""
    qty_distribute_agg = {'qty_dist': ('qty', 'count')}
    df_qty = df_invoice_masked[df_invoice_masked['qty'] != 0].groupby('qty').agg(**qty_distribute_agg).reset_index()
    sum_qty_dist = df_qty['qty_dist'].sum()
    df_qty['pct'] = (df_qty['qty_dist'] / sum_qty_dist * 100).round(1) if sum_qty_dist > 0 else 0
    df_qty.columns = ['Quantity Pocket', 'Quantity Distribution', '% Distribution']

    return df_qty

def get_df_atv_distrib(df_invoice_masked: pd.DataFrame):
    """ ## Invoice Value Distribution"""

    rev_col = 'revenue'
    atv_col = ['invoice', rev_col]
    df_atv = df_invoice_masked.loc[df_invoice_masked[rev_col] > 0, atv_col]

    revenue_map = {
        (0,         1_000_000)  : 'Under 1M',
        (1_000_000, 4_000_000)  : '1-4M',
        (4_000_000, 10_000_000) : '4-10M',
        (10_000_000, 20_000_000): '10-20M',
        (20_000_000, 30_000_000): '20-30M',
        (30_000_000, 50_000_000): '30-50M',
        (50_000_000, np.inf)    : 'Above 50M'
    }
    rev_bins = [k[0] for k in revenue_map.keys()] + [list(revenue_map.keys())[-1][1]]
    bin_labels = list(revenue_map.values())
    df_atv['rev_group'] = pd.cut(df_atv['revenue'], bins=rev_bins, labels=bin_labels)
    atv_dist = df_atv.groupby('rev_group', observed=False).size().reset_index()
    atv_dist.columns = ['Revenue Pocket', 'Revenue Distribution']
    sum_atv_dist = atv_dist['Revenue Distribution'].sum(axis=0)
    atv_dist['% Distribution'] =  (atv_dist['Revenue Distribution'] / sum_atv_dist * 100).round(1) if sum_atv_dist > 0 else 0

    return atv_dist

def get_df_traffic_distrib(fix_traffic: pd.DataFrame, period_anchor: pd.Timestamp):
    """ # Weekly Traffic Distribution """

    days_short = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    df_traffic: pd.DataFrame = fix_traffic[['Total Traffic']]
    df_traffic = df_traffic[df_traffic.index >= period_anchor]
    df_traffic = df_traffic.groupby(df_traffic.index.dayofweek).agg({'Total Traffic': 'mean'})
    sum_mean_traffic = df_traffic['Total Traffic'].sum()
    df_traffic['% Distribution'] = ((df_traffic['Total Traffic'] / sum_mean_traffic) * 100).round(1)
    df_traffic.index = df_traffic.index.map(lambda x: days_short[x])
    df_traffic = df_traffic.reset_index()

    return df_traffic

def get_round_month_df(
        period_double  : pd.DataFrame,
        period_regular : pd.DataFrame,
        period_anchor  : pd.Timestamp,
        date_config    : dict,
        period         : str,
):
    """
    ## rounded_month_df & month_year: str
    ### luôn lấy tròn n_tháng (date_option[0] = period_regular)
    """
    date_options = list(date_config.keys())

    if period == date_options[0]:
        rounded_month_df = period_regular
    else:
        # Vì month df không cần khóa ảnh hưởng advanced filter > dùng period_double làm intput
        month_mask, _ = get_dynamic_mask(period_double, period_anchor, period_mode = c.month)
        rounded_month_df = period_double.loc[month_mask].reset_index(drop=True)

    return rounded_month_df