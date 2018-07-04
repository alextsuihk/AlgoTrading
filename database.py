#!/usr/bin/python3

"""
This module query & update database
"""

# standard imports
#import decimal
import datetime as dt
import os

# third party imports
import numpy as np
import pandas as pd
import pymysql
from dotenv import load_dotenv
from sqlalchemy import create_engine

# local application imports
import constants
from log import write_log

# global setting
load_dotenv(dotenv_path='.env')

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASS = os.getenv('MYSQL_PASS')
MYSQL_DB = os.getenv('MYSQL_DB')

DB_ENGINE = create_engine('mysql+pymysql://' + MYSQL_USER + ':' \
    + MYSQL_PASS + '@' + MYSQL_HOST + ':' + MYSQL_PORT + '/' + MYSQL_DB)

DATABASE = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)

def parse_column_list(columns, raw=False):
    """
    convert daily column list to a string for MySQL query
    """
    ##column_string = ', '.join('{0}'.format(col) for col in columns)

    if raw:
        cond = 'raw'
    else:
        cond = 'raw, indicator, decision'

    tmp = cond.split()          # convert to list
    conds = [x.strip(',') for x in tmp] # strip out ','

    col_name_str = ''
    # column_names = []
    # column_types = []
    for column in columns:
        try:
            column[3]
        except IndexError:
            col_name_str = col_name_str + column[0] + ', '
        else:
            if column[3] in conds:
                col_name_str = col_name_str + column[0] + ', '

            # if only need raw-data, for non-raw-data, change to "default as column"
            else:
                if (column[1] == 'float') or (column[1] == 'int'):
                    col_name_str = col_name_str + 'Null as ' + column[0] + ', '
                elif column[1] == 'str':
                    col_name_str = col_name_str + '"' + column[2] + '" as ' + column[0] + ', '
                elif (column[1] == 'bool') and (column[2] == True):
                    col_name_str = col_name_str + '1 as ' + column[0] + ', '
                elif (column[1] == 'bool') and (column[2] == False):
                    col_name_str = col_name_str + '0 as ' + column[0] + ', '
                else:
                    write_log('N/A', 'N/A', '0000-00-00', 'parse_column_list', 'critical', \
                        'WRONG columns constant')

                # if column[1] == 'float':
                #     col_name_str = col_name_str + str(column[2]) + ' as ' + column[0] + ', '
                # elif column[1] == 'str':
                #     col_name_str = col_name_str + '"' + column[2] + '" as ' + column[0] + ', '
                # elif column[1] == 'bool' and column[2] == True:
                #     col_name_str = col_name_str + '1 as ' + column[0] + ', '
                # elif column[1] == 'bool' and column[2] == False:
                #     col_name_str = col_name_str + '0 as ' + column[0] + ', '
                # else:
                #     write_log('N/A', '0000-00-00', 'parse_column_list', 'critical', \
                #         'WRONG columns constant')

    # removing the last ', '
    col_name_str = col_name_str[: -2]

    return col_name_str


def create_empty_weekly_quote(symbol, year, workweek, weekday, today_open, today_date):
    """
    create single row empty (invalid) weekly_quote

    Args:
        param1(string): ticker, e.g. 00005
        param2(datetime.date): "2018-01-01"
        param3(int): workweek
        param4(int): weekday
        param5(int): today_open
        param6(int): today_date (assume today is the first day of the week)

    Returns:
        pandas: single row (weekly_df)
    """
    column_list = []

    empty_row = []                # create a 2-d list
    empty_row.append([])          # just need to 1st row

    for column in constants.WEEKLY_COLUMNS:
        column_list.append(column[0])       # for preparing Pandas column name

        # the following prepare a new row of data
        column_name = column[0]
        column_type = column[1]
        column_default = column[2]
        if column_name == 'symbol':
            empty_row[0].append(symbol)
        elif column_name == 'year':
            empty_row[0].append(year)
        elif column_name == 'workweek':
            empty_row[0].append(workweek)
        elif column_name == 'first_dow':
            empty_row[0].append(weekday)
        elif column_name == 'open':
            empty_row[0].append(today_open)
        elif column_name == 'first_day':
            empty_row[0].append(today_date)
        else:
            empty_row[0].append(column_default)
    
    return pd.DataFrame.from_records(empty_row, columns=column_list)


def delete_old_logs(day):
    """
    delete old logs more than X days

    Args:
        param1(int): number of days

    Returns:
        True for successful, False otherwise
    """
    ###############################################################################
    # execute SQL
    ###############################################################################
    day = max(0, day)       # cannot delete future

    start_date = dt.date.strftime((dt.datetime.today() - dt.timedelta(days=day)), '%Y-%m-%d')

    sql = "DELETE FROM logs WHERE created_at <= '%s 23:59:59'" % start_date
    cursor = DATABASE.cursor()
    cursor.execute(sql)
    DATABASE.commit()       # in case auto-commit is not enabled

    return True


def get_analysis_scenarios():
    # AT: Work-in-progress..........    
    """
    get analysis scenario parameter from database
    update status & analyzed_at
    if analyzed_at is over 5mins, and completed_at is blank restart

    Args:
        none

    Returns:
        pandas: parameters
    """
    # AT-Debug: from constants.SCENARIO_COLUMNS, generate 1st row of scenarios from default values
    col_names = []
    defaults = []
    defaults.append([])          # just need to 1st row

    for column in constants.SCENARIO_COLUMNS:
        col_names.append(column[0])
        defaults[0].append(column[2])

    scenarios = pd.DataFrame.from_records(defaults, columns=col_names)

    return scenarios
    ##########################################################################
    # Query ticker quote
    ##########################################################################
    # AT: under development   
    sql = 'SELECT id, a, b, c, d FROM scenarios \
           ORDER BY id asc'

    cursor = DATABASE.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()

    scenarios = np.array(results)

    return scenarios


def get_daily_quotes(market, symbol, trade_date, holidays, max_record, raw=False):
    """
    get (up to) 365 of the quotes prior to trade_date (skipping holidays as they don't have data)

    Args:
        param1(string): market
        param2(string): symbol, e.g. 00005
        param3(datetime.date): "2018-01-01"
        param4(list of string):list of holiday
        param5(int): max record to fetch from SQL

    Returns:
        pandas: quotes (daily_df)
    """
    column_string = parse_column_list(constants.DAILY_COLUMNS, raw)

    if max_record == 0:
        max_record = constants.MAX_DAILY_REOCORD       # max record to retrieve

    # this is unnecessary to convert datetime.date type to string, just want to be consistent
    trade_date = str(trade_date)

    # convert list to string for future MySQL query
    holidays_string = ', '.join('"{0}"'.format(h) for h in holidays)

    ##########################################################################
    # Query ticker quote
    ##########################################################################
    sql_prefix = "SELECT " + column_string + " FROM daily_raw "

    if holidays_string:
        sql_cond = "WHERE symbol ='%s' AND date <= '%s' AND date >= '%s' \
                AND date NOT IN (%s) \
                ORDER BY date ASC LIMIT %d" \
                % (symbol, trade_date, constants.SYSTEM_START_DATE, holidays_string, max_record)
    else:
        sql_cond = "WHERE symbol ='%s' AND date <= '%s' AND date >= '%s' \
                ORDER BY date ASC LIMIT %d" \
                % (symbol, trade_date, constants.SYSTEM_START_DATE, max_record)

    sql_statement = sql_prefix + sql_cond

    #df = pd.read_sql(sql_statement, con=db_engine, index_col="trade_date")
    df = pd.read_sql(sql_statement, con=DB_ENGINE)

    # replace "None" with np.nan
    df.fillna(value=np.nan, inplace=True)

    return df

    ##quotes = df.as_matrix()   
    ##return quotes

    # cursor = DATABASE.cursor()
    # cursor.execute(sql_statement)
    # results = cursor.fetchall()
    # quotes = np.array(results)

    # return quotes


def get_holidays(market, end_date, max_day):
    """
    query holiday list for past 365 days from end_date

    Args:
        param1(datetime.date): date of interested

    Returns:
        list of string: holiday
    """
    if max_day == 0:
        max_day = constants.MAX_HOLIDAY_REOCORD        # max record to retrieve

    start_date_str = dt.date.strftime((end_date - dt.timedelta(days=max_day)), '%Y-%m-%d')

    # this is unnecessary to convert datetime.date type to string, just want to be consistent
    end_date_str = str(end_date)

    ##########################################################################
    # Query holiday
    # if case some calculation need to skip holiday
    ##########################################################################
    sql = "SELECT date FROM holidays \
           LEFT JOIN markets on holidays.country = markets.country \
           WHERE date >= '%s' AND date <= '%s' ORDER BY date ASC" % (start_date_str, end_date_str)
    cursor = DATABASE.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()

    # convert holiday from list of datetime to list of string
    holidays = []
    for row in results:
        holidays.append(dt.datetime.strftime(row[0], '%Y-%m-%d'))

    return holidays


def get_previous_daily_quotes(market, symbol, trade_date, holidays, max_record, raw=False):
    """
    get (up to) 365 of the quotes prior to trade_date (skipping holidays as they don't have data)

    Args:
        param1(string): symbol, e.g. 00005
        param2(datetime.date): "2018-01-01"
        param3(list of string):list of holiday
        param4(int): max record to fetch from SQL

    Returns:
        pandas: quotes (daily_df)
    """
    # becasuse get_previous_daily_quotes() returns past quotes excluding today
    # we need to increment analysis_date by 1 day into future
    yesterday = trade_date - dt.timedelta(days=1)
    quotes = get_daily_quotes(market, symbol, yesterday, holidays, max_record, raw)
    return quotes


def get_weekly_quotes(market, symbol, trade_date, today_open, max_record):
    """
    query past (up to) 52 weeks weekly closing data (not including trade_date/today)

    Args:
        param1(string): market
        param2(string): ticker, e.g. 00005
        param3(datetime.date): "2018-01-01"
        param4(float): today-open-price
        param5(int): max record to fetch from SQL

    Returns:
        pandas: quotes (weekly_df)
    """
    column_string = parse_column_list(constants.WEEKLY_COLUMNS)

    if max_record == 0:
        max_record = constants.MAX_YEARLY_RECORD        # max record to retrieve

    # determine today's year & workweek
    year, workweek, weekday = trade_date.isocalendar()

    ##########################################################################
    # Query ticker quote (we don't care this week data, we will calculate anyway)
    ##########################################################################
    sql = "SELECT " + column_string + " FROM weekly WHERE symbol ='%s' \
        ORDER BY year DESC, workweek DESC LIMIT %d" % (symbol, max_record)

    cursor = DATABASE.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    quotes = np.array(results)

    # if quotes.shape[0] == 0 or quotes[0][1] != year or quotes[0][2] != workweek:
    #     invalid_weekly = create_empty_weekly_quote(symbol, year, workweek, weekday, today_open)

    #     if quotes.shape[0] == 0:
    #         quotes = invalid_weekly
    #     else:
    #         quotes = np.insert(quotes, 0, invalid_weekly, axis=0)

    # return quotes

    df = pd.read_sql(sql, con=DB_ENGINE)

    # generate current_week template if no previous data || cannot find this week data
    if df.shape[0] == 0 or df['year'][0] != year or df['workweek'][0] != workweek:
        invalid_weekly_df = create_empty_weekly_quote(symbol, year, workweek, weekday, \
            today_open, trade_date)

        # add the invalid_weekly to the TOP of the dataframe
        df = pd.concat([invalid_weekly_df, df])

    return df 
    ## quotes = df.as_matrix()
    ## return quotes


def get_ticker_priority():
    """
    ticker with high priority will be listed first
    ticker with priority =0, will NOT be return, hence, no processing

    Args:
        none

    Returns:
        list of mixed type: ['ticker', scenario_id]
    """
    ##########################################################################
    # Query ticker quote
    ##########################################################################
    sql = "SELECT symbol, scenario_id FROM tickers \
           WHERE priority > 0 \
           ORDER BY priority DESC, symbol ASC"

    cursor = DATABASE.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()

    #tickers = np.array(results)        # need to format scenario_id as int
    tickers = []
    for row in results:
        tickers.append([row[0], int(row[1])])   # need to convert scenario_id to int

    return tickers


def insert_multiple_daily_quote(market, symbol, quotes):
    """
    Development Module to insert MULTIPLE records into database

    Args:
        param1(string): symbol, e.g. 00005
        param3(numpy): quote (single row or multiple rows)

    Returns:
        True for successful, False otherwise
    """

    #########
    ### remove holiday records
    holidays = get_holidays(market, str(dt.date.today()), 365*15)
    ##########


    ###############################################################################
    # loop quotes
    ###############################################################################
    value = ""

    # multiple insert, concat multiple dates
    for quote in quotes:
        value = value + "('%s', '%s', %f, %f, %f, %f, %d)," \
            % (symbol, quote[0], quote[1], quote[2], quote[3], quote[4], quote[5])

    value = value[:-1]      # remove the trailing comma

    DC = constants.DAILY_COLUMNS

    if value != "":
        sql = 'INSERT INTO daily (symbol, date, open, high, low, close, volume) \
            VALUES %s' % value
        cursor = DATABASE.cursor()
        cursor.execute(sql)
        DATABASE.commit()       # in case auto-commit is not enabled
        return True

    return False

def recreate_quote_tables():
    """
    delete daily & weekly tables,
    and then re-create new tables (from constants.py column information)

    Args:
        None

    Returns:
        True for successful, False otherwise
    """
    ###############################################################################
    # DROP TABLE: daily & weekly
    ###############################################################################
    #sql = "DELETE FROM daily"
    sql = "DROP TABLE IF EXISTS daily"
    cursor = DATABASE.cursor()
    cursor.execute(sql)
    DATABASE.commit()       # in case auto-commit is not enabled

    #sql = "DELETE FROM weekly"
    sql = "DROP TABLE IF EXISTS weekly"
    cursor = DATABASE.cursor()
    cursor.execute(sql)
    DATABASE.commit()       # in case auto-commit is not enabled

    ###############################################################################
    # CREATE TABLE: daily & weekly
    ###############################################################################
    tables = ['daily', 'weekly']

    for table in tables:
        if table == 'daily':
            columns = constants.DAILY_COLUMNS
        else:
            columns = constants.WEEKLY_COLUMNS

        sql = 'CREATE TABLE ' + table + ' (id INTEGER AUTO_INCREMENT, modified_at DATETIME, '

        for column in columns:
            field_name = column[0]
            field_type = column[1]
            field_default = column[2]

            if field_type == 'bool':
                sql = sql + field_name + ' BOOLEAN DEFAULT ' + str(field_default) + ', '
            elif field_type == 'date':
                sql = sql + field_name + ' DATE DEFAULT "' + field_default + '", '
            elif field_type == 'float':
                sql = sql + field_name + ' FLOAT DEFAULT ' + str(field_default) + ', '
            elif field_type == 'int':
                sql = sql + field_name + ' INTEGER DEFAULT ' + str(field_default) + ', '
            elif field_type == 'str':
                sql = sql + field_name + ' VARCHAR(16) DEFAULT "' + field_default + '", '
            else:
                write_log('N/A', 'N/A', '0000-00-00', 'recreate_quote_tables', 'critical', \
                    'WRONG columns constant')

        if table == 'daily':
            sql = sql + 'PRIMARY KEY (id), INDEX (symbol), INDEX (date))'
        else:
            sql = sql + 'PRIMARY KEY (id), INDEX (symbol), INDEX (year, workweek))'

        cursor = DATABASE.cursor()
        cursor.execute(sql)

    return True


def upsert_daily_quote(quotes):
    """
    upsert data into database
        if the daily quote exists, update with NEW data (delete & insert)

    Args:
        param1(numpy): quotes (single row or multiple rows)

    Returns:
        True for successful, False otherwise
    """
    ###############################################################################
    # loop thru quotes (in case there are multiple days, when doing system init)
    ###############################################################################
    for quote in quotes:
        symbol = quote[0]
        trade_date = quote[1]

        # check if trade_date record already exists
        sql = "SELECT 'symbol', 'date' FROM 'daily' \
               WHERE 'symbol' ='%s' AND 'date' = '%s' LIMIT 1" % (symbol, trade_date)
        cursor = DATABASE.cursor()
        cursor.execute(sql)
        result = np.array(cursor.fetchall())
        record_exists = bool(result.shape[0] != 0)

        timestamp = dt.datetime.today().strftime('%Y-%m-%d %H:%M')

        if record_exists:
            sql_prefix = "UPDATE daily SET "
            i = 0

            sql_suffix = ''
            for column in constants.DAILY_COLUMNS:
                if column[1] == 'str':
                    sql_suffix = sql_suffix + column[0] + ' = "' + quote[i] + '", '
                elif column[1] == 'date':
                    sql_suffix = sql_suffix + column[0] + ' = "' + str(quote[i]) + '", '
                else:
                    sql_suffix = sql_suffix + column[0] + ' = ' + str(quote[i]) + ', '
                i += 1

            sql_suffix = sql_suffix + 'modified_at = "' + timestamp + '", '
            sql_suffix = sql_suffix[:-2]    # removing trailing ', '
            sql_condition = " WHERE 'symbol' ='%s' AND 'date' = '%s'" % (symbol, trade_date)
            sql = sql_prefix + sql_suffix + sql_condition

        else:
            column_string = parse_column_list(constants.DAILY_COLUMNS)
            column_string = column_string + ', modified_at '

            sql_prefix = "INSERT INTO daily "
            sql_suffix = "(" + column_string + ") VALUES ("
            i = 0
            for column in constants.DAILY_COLUMNS:
                if column[1] == 'str':
                    sql_suffix = sql_suffix + '"' + quote[i] + '", '
                elif column[1] == 'date':
                    sql_suffix = sql_suffix + '"' + str(quote[i]) + '", '
                else:
                    sql_suffix = sql_suffix + str(quote[i]) + ', '
                i += 1

            sql_suffix = sql_suffix[:-2]    # removing trailing ', '
            sql_suffix = sql_suffix + ', "' + timestamp + '")'
            sql = sql_prefix + sql_suffix

        cursor = DATABASE.cursor()
        cursor.execute(sql)
        DATABASE.commit()       # in case auto-commit is not enabled

    return True


def upsert_weekly_quote(quotes):
    """
    upsert data into database
        if the weekly quote exists, replace with NEW data (delete & insert)

    Args:
        param1(numpy): quotes (single row or multiple rows)

    Returns:
        True for successful, False otherwise
    """
    ###############################################################################
    # loop thru quotes (in case there are multiple days, when doing system init)
    ###############################################################################
    for quote in quotes:
        symbol = quote[0]
        year = quote[1]
        workweek = quote[2]

        # check if year+ww+weekday record already exists
        sql = "SELECT symbol FROM weekly \
               WHERE symbol = '%s' AND year = %d AND workweek = %d \
               LIMIT 1" % (symbol, year, workweek)
        cursor = DATABASE.cursor()
        cursor.execute(sql)
        result = np.array(cursor.fetchall())
        record_exists = bool(result.shape[0] != 0)

        timestamp = dt.datetime.today().strftime('%Y-%m-%d %H:%M')

        if record_exists:
            sql_prefix = "UPDATE weekly SET "
            i = 0

            sql_suffix = ''
            for column in constants.WEEKLY_COLUMNS:
                if column[1] == 'str':
                    sql_suffix = sql_suffix + column[0] + ' = "' + quote[i] + '", '
                elif column[1] == 'date':
                    sql_suffix = sql_suffix + column[0] + ' = "' + str(quote[i]) + '", '
                else:
                    sql_suffix = sql_suffix + column[0] + ' = ' + str(quote[i]) + ', '
                i += 1

            sql_suffix = sql_suffix[:-2]    # removing trailing ', '
            sql_condition = " WHERE symbol = '%s' AND year = %d AND workweek = %d "\
                % (symbol, year, workweek)
            sql = sql_prefix + sql_suffix + sql_condition

        else:
            column_string = parse_column_list(constants.WEEKLY_COLUMNS)
            column_string = column_string + ', modified_at '

            sql_prefix = "INSERT INTO weekly "
            sql_suffix = "(" + column_string + ") VALUES ("
            i = 0
            for column in constants.WEEKLY_COLUMNS:
                if column[1] == 'str':
                    sql_suffix = sql_suffix + '"' + quote[i] + '", '
                elif column[1] == 'date':
                    sql_suffix = sql_suffix + '"' + str(quote[i]) + '", '
                else:
                    sql_suffix = sql_suffix + '' + str(quote[i]) + ', '
                i += 1

            sql_suffix = sql_suffix[:-2]    # removing trailing ', '
            sql_suffix = sql_suffix + ', "' + timestamp + '")'
            sql = sql_prefix + sql_suffix

        cursor = DATABASE.cursor()
        cursor.execute(sql)
        DATABASE.commit()       # in case auto-commit is not enabled

    return True


def write_result(trade_date, symbol, scenario, duration, valid, result):
    """
    log analysis profile
    Args:
        param1(datetime.date): "2018-01-01"
        param2(string): ticker, e.g. 00005
        param3(int): analysis scenario
        param4(float): how long does analysis take
        param5(bool): is result valid
        param6(string): result (hopefully in JSON format)


    Returns:
        none
    """
    # this is unnecessary to convert datetime.date type to string, just want to be consistent
    trade_date = str(trade_date)

    ##########################################################################
    # insert into results
    ##########################################################################

    sql = "INSERT INTO results ('date', 'symbol', 'module', 'level', 'message') \
    VALUES ('%s', '%s', %d, %f, %s', '%s')" \
    % (trade_date, symbol, scenario, duration, valid, result)

    cursor = DATABASE.cursor()
    cursor.execute(sql)
    DATABASE.commit()       # in case auto-commit is not enabled

    return True
