#!/usr/bin/python3

"""
Initializing system for development purpose
Syntax: python3 initialization
"""

# standard imports
import datetime as dt
import logging
import time

# third party imports
#import numpy as np
from dotenv import load_dotenv

# local application imports
from database import delete_old_logs
from database import get_ticker_priority
from database import insert_multiple_daily_quote
from database import recreate_quote_tables
from fetch_quotes import fetch_quotes_all
from log import write_log

# global setting
load_dotenv(dotenv_path='.env')

def log(msg):
    """
    logging function, print screen & log to file
    """
    print(msg)                  # print to screen
    logging.info(msg.lstrip())  # write to log file
    write_log('_init', '0000-00-00', 'initialize', 'warning', msg.lstrip())


def main():
    """
    Initialize System: one time batch to fetech all historic data and analyze from start-date

    Args:
        None

    Returns:
        None

    """

    ###############################################################################
    # WARN user to reconfirm their action
    ###############################################################################
    print('\n\n')
    print('This Python script is to initialize (CLEAN & RE-FETECH) database')
    print('WARNING: You are about to EMPTY the daily & weekly quote databases\n')

    confirmation = input('PLEASE CONFIRM YOUR ACTION, and type "YES": ')
    if confirmation != 'YES':
        print('\nWe are not able to confirm your action, please try later')
        print('Good Bye\n')
        exit()

    print('We will wipe all quotes & performance analysis. \nAre you 100% sure ?')
    confirmation = input('Type "YES" to reconfirm: ')
    if confirmation != 'YES':
        print('\nWe are not able to reconfirm your action, please try later')
        print('Good Bye\n')
        exit()

    ###############################################################################
    # Enter start_date for analysis
    ###############################################################################
    #start_date = input("\nFor performance analysis, \nplease enter a start date (YYYY-MM-DD): "")

    ###############################################################################
    # setup log file
    ###############################################################################
    now = dt.datetime.today().strftime('%Y%m%d_%H%M')
    logfile = 'logs/init_'+ now + '.log'
    logging.basicConfig(filename=logfile, level=logging.INFO, \
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log('Started Initializing (today: %s)' % now)

    ###############################################################################
    # delete old logs
    ###############################################################################
    log('Deleting all logs')
    delete_old_logs(0)
    log("all logs are deleted")
    print("\n")

    ###############################################################################
    # delete & re-create Daily & Weekly tables
    ###############################################################################
    log("deleting & creating daily & weekly tables")
    recreate_quote_tables()
    log("daily & weekly tables are recreated")
    print("\n")

    ###############################################################################
    # get ticker priority
    ###############################################################################
    tickers = get_ticker_priority()

    for ticker in tickers:
        symbol = ticker[0]

        print("\n")
        start = time.perf_counter()
        log('Fetching symbol %s ' % (symbol))
        quotes = fetch_quotes_all(symbol)
        if quotes.shape[0] == 0:
            log('ERROR: Fail to fetch symbol [%s] history' % symbol)
        else:
            log('INFO: symbol [%s] fetches %d records in %f seconds' \
                % (symbol, quotes.shape[0], time.perf_counter() - start))

            insert_multiple_daily_quote(symbol, quotes)
            log('INFO: inserted symbol [%s] records into daily table (%f)' \
                % (symbol, time.perf_counter() - start))


# This is the standard bilerplate that calls that main() function.
if __name__ == "__main__":
    main()
