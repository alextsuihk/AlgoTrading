#!/usr/bin/python3

"""
This module query & update database
"""

# standard imports
import os

# third party imports
import pymysql
from dotenv import load_dotenv

# local application imports
import constants

# global setting
load_dotenv(dotenv_path='.env')

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_DB = os.getenv("MYSQL_DB")

DATABASE = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)

def write_log(market, symbol, trade_date, module, level, message):
    """
    write log into database

    Args:
        param1(string): market: HSI, NASQ, GOLD, USD-HKD, etc
        param2(string): ticker, e.g. 00005
        param3(datetime.date): e.g. "2018-01-01"
        param4(string): python module name
        param5(string): re-use Pytho logging levels
        param6(string): log message

        Python Loggiging Levels
        https://docs.python.org/3/library/logging.html

    Returns:
        none
    """
    # this is unnecessary to convert datetime.date type to string, just want to be consistent
    trade_date = str(trade_date)

    ##########################################################################
    # determine level
    ##########################################################################
    level = level.lower()       # convert to all lowercase
    if level == "debug":
        lvl = 20
    elif level == "info":
        lvl = 20
    elif level == "warning":
        lvl = 30
    elif level == "error":
        lvl = 40
    elif level == "critical":
        lvl = 50
    elif level == "fuckup":
        lvl = 99
    else:
        lvl = 0

    ##########################################################################
    # insert into logs
    ##########################################################################
    sql = "INSERT INTO logs (date, market, symbol, module, level, message) \
    VALUES ('%s', '%s', '%s', '%s', %d, '%s')" \
    % (trade_date, market, symbol, module, lvl, message)

    cursor = DATABASE.cursor()
    cursor.execute(sql)
    DATABASE.commit()       # in case auto-commit is not enabled

    return True
