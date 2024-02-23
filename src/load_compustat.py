import pandas as pd
from pandas.tseries.offsets import MonthEnd, YearEnd

import numpy as np
import wrds

import config
from pathlib import Path

OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)
WRDS_USERNAME = config.WRDS_USERNAME
# START_DATE = config.START_DATE
# END_DATE = config.END_DATE


description_compustat = {
    "gvkey": "Global Company Key",
    "datadate": "Data Date",
    "at": "Assets - Total",
    "pstkl": "Preferred Stock - Liquidating Value",
    "txditc": "Deferred Taxes and Investment Tax Credit",
    "pstkrv": "Preferred Stock - Redemption Value",
    # This item represents the total dollar value of the net number of
    # preferred shares outstanding multiplied by the voluntary
    # liquidation or redemption value per share.
    "seq": "Stockholders' Equity - Parent",
    "pstk": "Preferred/Preference Stock (Capital) - Total",
    "indfmt": "Industry Format",
    "datafmt": "Data Format",
    "popsrc": "Population Source",
    "consol": "Consolidation",
}


def pull_compustat(wrds_username=WRDS_USERNAME):
    """
    See description_compustat for a description of the variables.
    """
    sql_query = """
        SELECT 
            gvkey, datadate, at, pstkl, txditc,
            pstkrv, seq, pstk
        FROM 
            comp.funda
        WHERE 
            indfmt='INDL' AND -- industrial companies
            datafmt='STD' AND -- only standardized records
            popsrc='D' AND -- only from primary sources
            consol='C' AND -- consolidated financial statements
            datadate >= '01/01/1959'
        """
    # with wrds.Connection(wrds_username=wrds_username) as db:
    #     comp = db.raw_sql(sql_query, date_cols=["datadate"])
    db = wrds.Connection(wrds_username=wrds_username)
    comp = db.raw_sql(sql_query, date_cols=["datadate"])
    db.close()

    comp["year"] = comp["datadate"].dt.year
    return comp


description_crsp = {
    "permno": "Permanent Number - A unique identifier assigned by CRSP to each security.",
    "permco": "Permanent Company - A unique company identifier assigned by CRSP that remains constant over time for a given company.",
    "mthcaldt": "Calendar Date - The date for the monthly data observation.",
    "issuertype": "Issuer Type - Classification of the issuer, such as corporate or government.",
    "securitytype": "Security Type - General classification of the security, e.g., stock or bond.",
    "securitysubtype": "Security Subtype - More specific classification of the security within its type.",
    "sharetype": "Share Type - Classification of the equity share type, e.g., common stock, preferred stock.",
    "usincflg": "U.S. Incorporation Flag - Indicator of whether the company is incorporated in the U.S.",
    "primaryexch": "Primary Exchange - The primary stock exchange where the security is listed.",
    "conditionaltype": "Conditional Type - Indicator of any conditional issues related to the security.",
    "tradingstatusflg": "Trading Status Flag - Indicator of the trading status of the security, e.g., active, suspended.",
    "mthret": "Monthly Return - The total return of the security for the month, including dividends.",
    "mthretx": "Monthly Return Excluding Dividends - The return of the security for the month, excluding dividends.",
    "shrout": "Shares Outstanding - The number of outstanding shares of the security.",
    "mthprc": "Monthly Price - The price of the security at the end of the month.",
}


def pull_CRSP_stock_ciz(wrds_username=WRDS_USERNAME):
    """Pull necessary CRSP monthly stock data to
    compute Fama-French factors. Use the new CIZ format.
    """
    sql_query = """
        SELECT 
            a.mthcaldt, a.permno, a.ticker, a.shrout, a.mthprc
        FROM 
            crsp.msf_v2 AS a
        WHERE 
            a.mthcaldt BETWEEN '01/01/1988' AND '12/31/2019'
        """

    db = wrds.Connection(wrds_username=wrds_username)
    crsp_m = db.raw_sql(sql_query, date_cols=["mthcaldt"])
    db.close()

    return crsp_m

def pull_CRSP_SP_constituents(wrds_username=WRDS_USERNAME):
    """
    Pull all of the constituents PERMNO and tickers in the 
    expected timeframe.
    """
    sql_query = """
        SELECT a.permno, a.ticker
        FROM crsp.dsf AS a
        JOIN crsp.msenames AS b ON a.permno = b.permno
        WHERE b.namedt <= current_date
        AND b.nameendt >= current_date
        AND b.shrcd IN (10, 11)
        AND b.exchcd IN (1, 2, 3)
        AND EXISTS (
            SELECT *
            FROM crsp.msp500list AS c
            WHERE a.permno = c.permno
            AND c.start <= current_date
            AND c.end >= current_date
        )
        """