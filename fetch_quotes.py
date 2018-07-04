#!/usr/bin/python3

"""
Fetch eod (End of Day) Stock Quote from Quandl API, and insert into database
"""

# standard imports
import datetime
import json
import os

# third party imports
import numpy as np
import requests
from dotenv import load_dotenv

# local application imports
from database import upsert_daily_quote
from log import write_log


# global setting
load_dotenv(dotenv_path='.env')


###############################################################################
# Linter Disable/Ignore
###############################################################################
"""
# pylint: disable=C0103
"""

def fetch_quote(symbol, trade_date, realtime):
    """
    Fetch Quote Data online, and insert/update into database

    Args:
        param1(string): ticker   e.g. 00005
        param2(string): date || all || null    # foramt=YYYY-MM-DD, use today if null

    Returns:
        bool: True for success, False otherwse

    """
    ###############################################################################
    # Pass Parameter & load .env
    ###############################################################################
    quotes = fetch_quandl(symbol)

    if quotes.shape[0] != 0:
        ### AT: please format to proper shape (1, columns)
        upsert_daily_quotes(symbol, trade_date, quotes)
        return True

    return False            # if quotes is empty, return error

def fetch_quotes_all(symbol):
    """
    for initializng daily quotes (per symbol), fetching all quote history

    Args:
        param1(string): symbol   e.g. 00005

    Returns:
        bool: True for success, False otherwse

    """

    # AT: check and properly swapping column locaton   
    return fetch_quandl(symbol)


def fetch_quandl(symbol):

    ## return a Pandas dataframe, rename columne if needed (df.rename(columns = {'old': 'new'}, inplace=True))
    # new_cols = ['new1', 'new2', 'new3']
    # df.columns = new_cols
    # 
    # override column
    # df = pf.read_csv('file', names=new_cols, header=0)
    # df.drop('unused_col', axis=1, inplace=True)
    ## lower case
    """
    Fetch JSON data from server, quandl JSON contains a long long record
    Note: quandl fetches ALL dates
    Quandl ONLY provides offline (end of day) data

    Args:
        param1(string): symbol   e.g. 00005

    Returns:
        numpy (after converting from JSON)
        NOTE: remove rows with volume == 0
    """
    ###############################################################################
    # Format symbol to quandl
    # quandl format is 5-digits with leading zeros
    ###############################################################################
    symbol = symbol.lstrip("0").zfill(5)

    ###############################################################################
    # Fetch data
    ###############################################################################
    api_key = os.getenv("QUANDL_API_KEY")
    url = 'https://www.quandl.com/api/v3/datasets/XHKG/' + symbol + '/data.json?api_key=' + api_key

    try:
        response = requests.get(url) # pylint: disable=invalid-name

    except ConnectionError as error:
        print('error when accessing Quandl API: ', error)
        exit(1)

    fetched = response.content.decode('utf-8')
    tmp_dict = json.loads(fetched)              # convert to dict
    column_names = tmp_dict['dataset_data']['column_names']   #
    quotes = tmp_dict['dataset_data']['data']   #
    quotes = np.array(quotes)                 # convert to numpy

    # remove record with volume is zero
    valids = quotes[:, 5] != 0                  # valid if volume is non-zero
    quotes = quotes[valids]                     # extract & keep ONLY the valid rows

    return quotes
