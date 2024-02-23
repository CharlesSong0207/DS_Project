from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

import numpy as np
import pandas as pd
import wrds

import config

DATA_DIR = Path(config.DATA_DIR)
WRDS_USERNAME = config.WRDS_USERNAME
START_DATE = config.START_DATE
END_DATE = config.END_DATE

def pull_CRSP_sp_return_monthly(
        start_date=START_DATE,
        end_date=END_DATE,
        wrds_username=WRDS_USERNAME
):
    
    query = f"""
    SELECT caldt, sprtrn
    FROM crsp.msp500
    WHERE caldt BETWEEN '1988-01-01' AND '2019-12-31'
    """

    db = wrds.Connection(wrds_username = wrds_username)
    df = db.raw_sql(
        query, date_cols=["caldt"]
    )
    db.close()

    df['log_return'] = np.log1p(df['sprtrn'])
    return df

def calc_monthly_return(data):
    # Convert the 'date' column to datetime format
    data['caldt'] = pd.to_datetime(data['caldt'])
    data.set_index('caldt', inplace=True)
    monthly_log_returns = data['log_return'].resample('M').sum()

    return monthly_log_returns

def calc_annual_return(data):
    data['caldt'] = pd.to_datetime(data['caldt'])

    # Set the 'date' as the index
    data.set_index('caldt', inplace=True)

    # Group by year and month, and select the last day
    df_mkt_month_end = data.resample('M').last()

    # Calculate the rolling 12-month sum of log returns
    df_mkt_month_end['annual_log_return'] = df_mkt_month_end['log_return'].rolling(window=12).sum()

    # Display the total number of observations and the first few rows to verify the rolling calculation
    mkt_annual_ret = df_mkt_month_end['annual_log_return'].fillna(0)

    return mkt_annual_ret