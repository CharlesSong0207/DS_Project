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

def pull_CRSP_Fama_French_monthly(
        start_date=START_DATE,
        end_date=END_DATE,
        wrds_username=WRDS_USERNAME
):
    
    query = f"""
    SELECT date, mktrf, rf
    FROM ff.factors_daily
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """

    db = wrds.Connection(wrds_username = wrds_username)
    df = db.raw_sql(
        query, date_cols=["date"]
    )
    db.close()
    return df

def get_Fama_French_Mkt_Return(data):
    data['mkt'] = data['mktrf'] + data['rf']
    df = data[['date','mkt']]
    df['log_return'] = np.log1p(df['mkt'])

    return df

def calc_monthly_return(data):
    # Convert the 'date' column to datetime format
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date', inplace=True)
    monthly_log_returns = data['log_return'].resample('M').sum()

    return monthly_log_returns

def calc_annual_return(data):
    data['date'] = pd.to_datetime(data['date'])

    # Set the 'date' as the index
    data.set_index('date', inplace=True)

    # Group by year and month, and select the last day
    df_mkt_month_end = data.resample('M').last()

    # Calculate the rolling 12-month sum of log returns
    df_mkt_month_end['annual_log_return'] = df_mkt_month_end['log_return'].rolling(window=12).sum()

    # Display the total number of observations and the first few rows to verify the rolling calculation
    mkt_annual_ret = df_mkt_month_end['annual_log_return'].fillna(0)

    return mkt_annual_ret


#def pull_Fama_French_Return_Table():
#    df = web.DataReader('F-F_Research_Data_Factors', 'famafrench')
#    df = df[0]

#    return df

#def calc_Fama_French_Mkt_Return(df):
#    df['Mkt'] = df['Mkt-RF'] + df['RF']
#    df_mkt = df['Mkt'].to_frame()

#    return df_mkt