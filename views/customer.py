from src.columns import colName as c
from src import process_customer_info
from core import bf_fill
import streamlit as st
import pandas as pd
import numpy as np
is_james = st.query_params.get('authorize') == st.secrets.env.start_lab
SS = st.session_state
st.title('🤬 Customer')
def dedash(df: pd.DataFrame):
    res = df.copy()
    str_cols = res.select_dtypes(include=['string', 'object']).columns
    res[str_cols] = res[str_cols].replace('-', pd.NA)
    return res
full_sales: pd.DataFrame = SS.get('analysis_sales', None)
if full_sales is None:
    st.info('Switch to dashboard then switch back.')
    st.stop()

requires    = [c.date, c.invoice, c.cat, c.revenue, c.cus_id, c.cus_name, c.cus_email]
sku_mask    = lambda df: df[c.sku] != '-'
qty_mask    = lambda df: df[c.qty]  >  0
sales       = full_sales.loc[sku_mask(full_sales) & qty_mask(full_sales), requires].pipe(dedash)
sales       = bf_fill(sales, c.invoice, [c.cus_id, c.cus_name, c.cus_email]).dropna(subset=c.cus_id, ignore_index=True)
sales       = process_customer_info(sales)

customer_group = sales.groupby(c.cus_id, as_index=False)
today          = pd.Timestamp.today().normalize()
agg_config     = {
    c.date      : (c.date, 'nunique'),
    c.revenue   : (c.revenue, 'sum'),
    c.cus_name  : (c.cus_name, 'first'),
    c.cus_email : (c.cus_email, 'first'),
    'last_visit': (c.date, lambda x: (today - x.max()).days)
}
customer = customer_group.agg(**agg_config).sort_values(by=c.date, ascending=False, ignore_index=True)
customer['revenue_per_visit'] = (customer[c.revenue] / customer[c.date]).astype('int64')

column_config = {
    c.cus_id    : st.column_config.TextColumn('Phone Number'),
    c.invoice   : st.column_config.NumberColumn('Invoice'),
    c.date      : st.column_config.NumberColumn('Visit'),
    'last_visit'      : st.column_config.NumberColumn('Since Last Visit'),
    'revenue_per_visit' : st.column_config.NumberColumn('Revenue Per Visit', format='%,.0f'),
    c.revenue   : st.column_config.NumberColumn('Spent', format='%,.0f'),
    c.cus_name  : st.column_config.TextColumn('Name'),
    c.cus_email : st.column_config.TextColumn('Email'),
}
st.dataframe(customer, column_config=column_config, height=800)
