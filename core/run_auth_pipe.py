from core import *
from src import colRaw as r
from src.datetime_logic import week_month_year
from contextlib import redirect_stdout
from pathlib import Path
import streamlit as st
import pandas as pd
import io
import os

config = {
    # config chung cho pipeline
    'payment_cols'      : [r.cash, r.card, r.payoo, r.banking, r.mkt, r.vnpay, r.trade_in],
    'disc_cols'         : [r.disc_pct, r.disc_amt],
    'date_anchor'       : r.invoice,
    'anonymous'         : [r.cus_id, r.cus_name, r.cus_email]
}

@st.cache_data(show_spinner='Fetching & cleaning data from Google Sheets')
def authentic_pipeline(sales_raw: pd.DataFrame)-> pd.DataFrame:
    """
    PIPELINE AUTHENTIC
    """

    log = io.StringIO()
    with redirect_stdout(log):
        df = (sales_raw
            .pipe(smart_sales_clean,
                config = config
            )
            .pipe(bf_fill, 
                _anchor = r.invoice, 
                _target_cols = config['anonymous']
            )
            .pipe(week_month_year)
        )

    assert isinstance(df, pd.DataFrame), 'show docstring'
    return df


if __name__ == "__main__":
    test = authentic_pipeline()
    print(test.columns)