#!/usr/bin/python3

"""
This module read RAW daily_quotes data, and it does NOT update database,
results are written to CSV files
"""
# standard imports
import datetime as dt
import logging
import sys
import time

# third party imports
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# local application imports
import analysis
import constants
from database import create_empty_weekly_quote
from database import get_analysis_scenarios
from database import get_daily_quotes
from database import get_holidays
#from database import get_ticker_priority
from database import upsert_daily_quote
from database import upsert_weekly_quote
from indicators import is_bearish_engulfing
from indicators import is_bullish_engulfing
from indicators import is_bearish_pin_bar
from indicators import is_bullish_pin_bar
from indicators import is_dark_cloud
from indicators import is_piercing_pattern
from log import write_log

# global setting
load_dotenv(dotenv_path='.env')

MAX_RECORD = 365*15
# def log(msg):
#     """
#     logging function, print screen & log to file
#     """
#     ##print(msg)                  # print to screen
#     logging.info(msg.lstrip())  # write to log file
#     write_log('_init', '0000-00-00', 'initialize', 'warning', msg.lstrip())


def main():
    """
    main function
    """
    ###############################################################################
    # get simulation parameters
    # Note: For future multi-processing, add [timestamp] when writing log
    ###############################################################################
    #simulations = get_simulations()
    market = 'HSI'
    symbol = '00005'
    scene_id = 0
    begin_date = '2016-01-01'
    end_date = '2018-04-10'
    simulations = [[market, symbol, scene_id, begin_date, end_date]]

    ###############################################################################
    # parse "realtime", to fetch data using realtime fashion
    ###############################################################################
    # try:
    #     trade_date = str(datetime.datetime.strptime(argv[1], '%Y-%m-%d').date())
    # except (IndexError, ValueError):
    #     trade_date = datetime.datetime.today().strftime('%Y-%m-%d')
    #     write_log('', N/A', trade_date, 'daily_task', 'warning', \
    #         'passing invalid date to daily_task.py, using today date instead')


    ###############################################################################
    # setup log file
    ###############################################################################
    now = dt.datetime.today().strftime('%Y%m%d_%H%M%S')

    # logfile = 'logs/init_'+ now + '.log'
    # logging.basicConfig(filename=logfile, level=logging.INFO, \
    #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    daily_file = './sim_results/' + now + '_daily.xlsx'
    weekly_file = './sim_results/' + now + '_weekly.xlsx'
    trade_file = './sim_results/' +now + '_trade.xlsx'
    performance_file = './sim_results/' +now + '_perform.xlsx'

    writer_daily = pd.ExcelWriter(daily_file)
    writer_weekly = pd.ExcelWriter(weekly_file)
    write_trade = pd.ExcelWriter(trade_file)
    write_performance = pd.ExcelWriter(performance_file)

    ###############################################################################
    # Get analysis parameter (matrix)
    ###############################################################################
    scenes = get_analysis_scenarios()

    ###############################################################################
    # get holidays in list of string
    ###############################################################################
    holidays = get_holidays(market, dt.date.today(), MAX_RECORD)

    ##########################################################################
    # create empty (zero length) profits dataframe with proper data-types
    ##########################################################################
    col_names = []
    # defaults = []
    # defaults.append([])          # just need to 1st row

    for column in constants.TRADE_COLUMNS:
        col_names.append(column[0])
        # defaults[0].append(column[2])

    trades = pd.DataFrame(columns=col_names)
    # trades = pd.DataFrame.from_records(defaults, columns=col_names)
    # trades.drop(0, axis=0, inplace=True)

    ##########################################################################
    # create empty (zero length) performances dataframe with proper data-types
    ##########################################################################
    col_names = []
    defaults = []
    defaults.append([])          # just need to 1st row

    for column in constants.PERFORMANCE_COLUMNS:
        col_names.append(column[0])
        defaults[0].append(column[2])

    performance = pd.DataFrame.from_records(defaults, columns=col_names)
    performance.drop(0, axis=0, inplace=True)

    ###############################################################################
    # Loop thru each simulation
    # NOTE: AT-Pending: will enable Multi-Processing
    ###############################################################################
    for simulation in simulations:
        start = time.perf_counter()             # log the performance

        # destructure simiulation info
        market, symbol, scene_id, begin_date, end_date = simulation

        if not end_date:
            end_date = str(dt.date.today())

        msg = '[%s] Processing symbol %s (scenario ID: %d)' % (now, symbol, scene_id)
        write_log(market, symbol, dt.date.today(), 'simulations', 'info', msg)

        ###############################################################################
        # strip out ALL calculated data, just keep the raw data, get ENTIRE daily
        ###############################################################################
        raw_only = True
        daily = get_daily_quotes(market, symbol, str(dt.date.today()), holidays, MAX_RECORD, raw_only)
        #daily_all.set_index('date', inplace=True)

        ## find out the begin_idx
        ## find 1st row with true_range is null


        # AT-Pending: 
        print('Get Daily Quotes: ', time.perf_counter() - start)

        # extract trading-date from entire daily list
        trade_dates = daily['date'].tolist()

        ###############################################################################
        # advance end_date to next calendar date to cover the end of array
        # Python end-index is excluded, so, need to advance by 1
        ###############################################################################
        end_date = str(dt.datetime.strptime(end_date, '%Y-%m-%d').date() + dt.timedelta(days=1))
        end_date_obj = dt.datetime.strptime(end_date, '%Y-%m-%d').date()

        # advance one day if begin_date is weekend or non-trading date
        while True:
            begin_date_obj = dt.datetime.strptime(begin_date, '%Y-%m-%d').date()
            weekday = begin_date_obj.isocalendar()[2]
            cond = weekday != 6 and weekday != 7 and (begin_date not in holidays) \
                and (begin_date_obj in trade_dates)
            if cond:
                break

            # advance one day if Sat or Sun or it is a holiday, or there is no trading data
            begin_date_obj = begin_date_obj + dt.timedelta(days=1)
            begin_date = str(begin_date_obj)

        # advance one day if end_date is weekend or non-trading date
        while True:
            end_date_obj = dt.datetime.strptime(end_date, '%Y-%m-%d').date()
            weekday = end_date_obj.isocalendar()[2]
            cond = weekday != 6 and weekday != 7 and (end_date not in holidays) \
                and (end_date_obj in trade_dates)
            if cond:
                break

            # advance one day if Sat or Sun or it is a holiday, or there is no trading data
            end_date_obj = end_date_obj + dt.timedelta(days=1)
            end_date = str(end_date_obj)

        # calculate the begin & end index (for extracting daily)
        begin_idx = trade_dates.index(begin_date_obj)
        end_idx = trade_dates.index(end_date_obj)

        # like for wrong user configuration
        if begin_date > end_date:
            msg = '[%s] cannot match trade_date begin_idx %s (date: %s)' \
                % (now, symbol, str(begin_date_obj))
            write_log(market, symbol, dt.date.today(), 'simulations', 'critical', msg)
            continue

            ## AT-Pending: no longer useful, so delete it   
        ###############################################################################
        # generate a range of analysis_date, don't worry about non-trading days,
        # will skip non-trading-days & holiday in the loop
        ###############################################################################
        analysis_dates = np.arange(begin_date, end_date, dtype='datetime64[D]').tolist()

        ###############################################################################
        # calculate Daily Indicators
        # NOTE:
        #  1. daily dataframe, date is in reverse order (newest date first)
        #  2. Only calculate the subset of daily DF
        ###############################################################################
        # point to subbet of daily data which need to be computed
        daily_open = daily['open'].iloc[begin_idx:end_idx]
        daily_high = daily['high'].iloc[begin_idx:end_idx]
        daily_low = daily['low'].iloc[begin_idx:end_idx]
        daily_close = daily['close'].iloc[begin_idx:end_idx]
        daily_volume = daily['volume'].iloc[begin_idx:end_idx]

        # calculate daily fast & slow EMA
        period = scenes['daily_ema_slow'].iloc[scene_id]
        daily['ema_slow'] = daily_close.ewm(span=period, min_periods=period, adjust=False).mean()
        daily_ema_slow = daily['ema_slow'].iloc[begin_idx:end_idx]

        period = scenes['daily_ema_fast'].iloc[scene_id]
        daily['ema_fast'] = daily_close.ewm(span=period, min_periods=period, adjust=False).mean()
        daily_ema_fast = daily['ema_fast'].iloc[begin_idx:end_idx]

        # calculate fast & slow EMA slope
        ema_slow_slope = daily['ema_slow_slope'].iloc[begin_idx:end_idx]
        ema_slow_slope[daily_ema_slow > daily_ema_slow.shift(1)] = 'rising'
        ema_slow_slope[daily_ema_slow < daily_ema_slow.shift(1)] = 'falling'
        ema_slow_slope[daily_ema_slow == daily_ema_slow.shift(1)] = 'flat'
        #daily['ema_slow_slope'] = ema_slow_slope

        ema_fast_slope = daily['ema_fast_slope'].iloc[begin_idx:end_idx]
        ema_fast_slope[daily_ema_fast > daily_ema_fast.shift(1)] = 'rising'
        ema_fast_slope[daily_ema_fast < daily_ema_fast.shift(1)] = 'falling'
        ema_fast_slope[daily_ema_fast == daily_ema_fast.shift(1)] = 'flat'
        #daily['ema_fast_slope'] = ema_fast_slope

        # calculate MACD
        period = scenes['macd_fast_period'].iloc[scene_id]
        macd_ema_fast = daily_close.ewm(span=period, min_periods=period, adjust=False).mean()

        period = scenes['macd_slow_period'].iloc[scene_id]
        macd_ema_slow = daily_close.ewm(span=period, min_periods=period, adjust=False).mean()

        daily['macd_line'] = macd_ema_fast - macd_ema_slow
        macd_line = daily['macd_line'].iloc[begin_idx:end_idx]

        # calculate MACD signal_line
        period = scenes['macd_signal_line_period'].iloc[scene_id]
        daily['signal_line'] = macd_line.ewm(span=period, min_periods=period, adjust=False).mean()
        signal_line = daily['signal_line'].iloc[begin_idx:end_idx]

        # calculate MACD histogram
        daily['macd_hist'] = macd_line - signal_line
        macd_hist = daily['macd_hist'].iloc[begin_idx:end_idx]

        # calculate MACD histogram slope
        macd_hist_slope = daily['macd_hist_slope'].iloc[begin_idx:end_idx]
        macd_hist_slope[macd_hist > macd_hist.shift(1)] = 'rising'
        macd_hist_slope[macd_hist < macd_hist.shift(1)] = 'falling'
        macd_hist_slope[macd_hist == macd_hist.shift(1)] = 'flat'

         # calculate Impulse Color
        daily_impulse_color = daily['impulse_color'].iloc[begin_idx:end_idx]
        cond = ema_fast_slope + '-' + macd_hist_slope  # dirty way to concat two columns

        daily_impulse_color[cond == 'rising-rising'] = 'green'
        daily_impulse_color[cond == 'falling-falling'] = 'red'
        daily_impulse_color[cond == 'falling-rising'] = 'blue'
        daily_impulse_color[cond == 'rising-falling'] = 'blue'
        daily_impulse_color[cond == 'rising-flat'] = 'blue'
        daily_impulse_color[cond == 'flat-rising'] = 'blue'
        daily_impulse_color[cond == 'flat-flat'] = 'yellow'

        # calculate Force Index
        daily['force_index'] = (daily_close - daily_close.shift(1)) * daily_volume
        force_index = daily['force_index'].iloc[begin_idx:end_idx]

        # calculate Force Index(2)
        period = 2
        daily['force_index2'] = force_index.ewm(span=period, min_periods=period, adjust=False).mean()
        daily_force_index2 = daily['force_index2'].iloc[begin_idx:end_idx]

        # calculate Force Index(13)
        period = 13
        daily['force_index13'] = force_index.ewm(span=period, min_periods=period, adjust=False).mean()
        daily_force_index13 = daily['force_index13'].iloc[begin_idx:end_idx]

        # calculate True Range
        daily['true_range'] = pd.concat([abs(daily_low - daily_close.shift(1)), \
            abs(daily_high - daily_close.shift(1)), \
            abs(daily_high - daily_low)], axis=1).max(axis=1)
        true_range = daily['true_range'].iloc[begin_idx:end_idx]

        # calculate ATR (Average True Range)
        period = scenes['atr_period'].iloc[scene_id] - 1
        daily['atr'] = true_range.ewm(com=period, min_periods=period, adjust=False).mean()
        daily_atr = daily['atr'].iloc[begin_idx:end_idx]

        # calculate Value Zone
        ema_fast = daily['ema_fast'].iloc[begin_idx:end_idx]
        ema_slow = daily['ema_slow'].iloc[begin_idx:end_idx]
        high_boundary = pd.concat([ema_fast, ema_slow], axis=1).max(axis=1)
        low_boundary = pd.concat([ema_fast, ema_slow], axis=1).min(axis=1)

        daily_value_zone = daily['value_zone'].iloc[begin_idx:end_idx]
        daily_value_zone[(daily_close <= high_boundary) & (daily_close >= low_boundary)] = 'inside'
        daily_value_zone[daily_close > high_boundary] = 'above'
        daily_value_zone[daily_close < low_boundary] = 'below'
        #daily['value_zone'] = daily_value_zone

        # calculate Bearish Engulfing (proprietary)
        daily['bearish_engulfing'] = is_bearish_engulfing(daily_close, daily_open)
        daily_bearish_engulfing = daily['bearish_engulfing'].iloc[begin_idx:end_idx]

        # calculate Bullish Engulfing (proprietary)
        daily['bullish_engulfing'] = is_bullish_engulfing(daily_close, daily_open)
        daily_bullish_engulfing = daily['bullish_engulfing'].iloc[begin_idx:end_idx]

        # calculate Bearish Pin Bar (proprietary)
        daily['bearish_pin_bar'] = is_bearish_pin_bar(daily.iloc[begin_idx:end_idx], \
            scenes.iloc[scene_id])
        daily_bearish_pin_bar = daily['bearish_pin_bar'].iloc[begin_idx:end_idx]

        # calculate Bullish Pin Bar (proprietary)
        daily['bullish_pin_bar'] = is_bullish_pin_bar(daily.iloc[begin_idx:end_idx], \
            scenes.iloc[scene_id])
        daily_bullish_pin_bar= daily['bullish_pin_bar'].iloc[begin_idx:end_idx]

        # calculate Dark Cloud
        daily['dark_cloud'] = is_dark_cloud(daily.iloc[begin_idx:end_idx], \
            scenes.iloc[scene_id])
        daily_dark_cloud = daily['dark_cloud'].iloc[begin_idx:end_idx]

        # calculate Piercing Pattern (proprietary)
        daily['piercing_pattern'] = is_piercing_pattern(daily.iloc[begin_idx:end_idx], \
            scenes.iloc[scene_id])
        daily_piercing_pattern = daily['piercing_pattern'].iloc[begin_idx:end_idx]

        # calculate csize daily
        daily_avg = daily['ema_slow'].iloc[begin_idx:end_idx]
        max_diff = pd.concat([abs(daily_high - daily_avg), abs(daily_low - daily_avg)], axis=1).max(axis=1)
        max_diff = 2 * max_diff / daily_avg

        ## AT-Debug: save to excel, so it could be verified  
        daily['max_diff'] = max_diff

        factor = scenes['csize_factor'].iloc[scene_id]
        window = scenes['csize_window'].iloc[scene_id]

        daily_csize = max_diff.rolling(window).std() * factor/10
        daily['csize'] = daily_csize

        # determine pandas series boolean pointing to 1st trading & last trading day of the week
        daily_date = daily.date.iloc[begin_idx:end_idx]
        daily_datetime = pd.to_datetime(daily_date)
        daily_year = daily_datetime.dt.year
        daily_week = daily_datetime.dt.week
        daily_weekday = daily_datetime.dt.weekday + 1

        start_of_week = daily_week != daily_week.shift(1)
        end_of_week = daily_week != daily_week.shift(-1)

        # extract weekly_csize
        # will use csize from LAST FRIDAY (last trading day of last week), and populate into this week csize   
        weekly_csize = daily['weekly_csize'].iloc[begin_idx:end_idx]
        weekly_csize[start_of_week] = daily_csize.shift(1)
        weekly_csize.ffill(inplace=True)                # forward fill the remaing of the week

        # calculate auto envelope channel
        channel = weekly_csize * daily_avg
        daily['ae_upper_channel'] = daily_avg + channel /2
        daily['ae_lower_channel'] = daily_avg - channel /2
        daily_ae_upper_channel = daily['ae_upper_channel'].iloc[begin_idx:end_idx]
        daily_ae_lower_channel = daily['ae_lower_channel'].iloc[begin_idx:end_idx]

        # calculate Force Index with ATR channel
        fi_atr_upper_ch_multiplier = scenes['fi_atr_upper_channel_multiplier'].iloc[scene_id]
        fi_atr_lower_ch_multiplier = scenes['fi_atr_lower_channel_multiplier'].iloc[scene_id]
        daily['force_index_atr_upper_ch'] = daily_force_index13 - fi_atr_upper_ch_multiplier * daily_atr
        daily['force_index_atr_lower_ch'] = daily_force_index13 - fi_atr_lower_ch_multiplier * daily_atr
        daily_force_index_atr_upper_ch = daily['force_index_atr_upper_ch'].iloc[begin_idx:end_idx]
        daily_force_index_atr_lower_ch = daily['force_index_atr_lower_ch'].iloc[begin_idx:end_idx]

        # AT-Pending: 
        print('Get Pure Daily Computation: ', time.perf_counter() - start)


        ###############################################################################
        # extract & formulate weekly_close
        ###############################################################################
        # convert from float to object
        daily['weekly_close'] = ''

        weekly_ema_slow_period = scenes['weekly_ema_slow'].iloc[scene_id]
        weekly_ema_fast_period = scenes['weekly_ema_fast'].iloc[scene_id]
        weekly_ema_ema_fast_period = scenes['macd_fast_period'].iloc[scene_id]

        # extract daily_close from every friday (last trading day of the week)
        for index, value in daily_close.iteritems():

            # extract daily_close including today (in case today is Monday)
            recent_daily_close = daily['close'].iloc[begin_idx:index]

            # extract PREVIOUS (not including this week) weekly_close
            recent_weekly_close = recent_daily_close[end_of_week].reset_index(drop=True)

            # IF NOT friday, append today_close (treat as this weekly close)
            if daily['date'].iloc[index] not in end_of_week:
                today_close = pd.Series(daily['close'].iloc[index], index=[recent_weekly_close.shape[0]])
                recent_weekly_close = pd.concat([recent_weekly_close, today_close], axis=0)

            # AT-Debug: save recent_weekly_close to Excel
            daily['weekly_close'].update(pd.Series([recent_weekly_close.values], index=[index]))
            #daily_weekly_close = daily['weekly_close'].iloc[begin_idx:index]

            # calculate weekly slow EMA
            ema_slow = recent_weekly_close.ewm(span=weekly_ema_slow_period, \
                min_periods=weekly_ema_slow_period, adjust=False).mean()
            daily['weekly_ema_slow'].update(pd.Series([ema_slow.iloc[-1]], index=[index]))

            # calculate weekly fast EMA
            ema_fast = recent_weekly_close.ewm(span=weekly_ema_fast_period, \
                min_periods=weekly_ema_fast_period, adjust=False).mean()
            daily['weekly_ema_fast'].update(pd.Series([ema_fast.iloc[-1]], index=[index]))

            # calculate weekly MACD slow EMA (period = 26) [re-using weekly_ema_slow]
            daily['weekly_macd_ema_slow'].update(pd.Series([ema_slow.iloc[-1]], index=[index]))

            # calculate weekly MACD fast EMA (period = 12)
            ema_fast = recent_weekly_close.ewm(span=weekly_ema_ema_fast_period, \
                min_periods=weekly_ema_ema_fast_period, adjust=False).mean()
            daily['weekly_macd_ema_fast'].update(pd.Series([ema_fast.iloc[-1]], index=[index]))


        # calulate weekly slow EMA slope
        weekly_ema_slow = daily['weekly_ema_slow'].iloc[begin_idx:end_idx]
        weekly_ema_slow_slope = daily['weekly_ema_slow_slope'].iloc[begin_idx:end_idx]
        weekly_ema_slow_slope[weekly_ema_slow > weekly_ema_slow.shift(1)] = 'rising'
        weekly_ema_slow_slope[weekly_ema_slow < weekly_ema_slow.shift(1)] = 'falling'
        weekly_ema_slow_slope[weekly_ema_slow == weekly_ema_slow.shift(1)] = 'flat'

        # calulate weekly fast EMA slope
        weekly_ema_fast = daily['weekly_ema_fast'].iloc[begin_idx:end_idx]
        weekly_ema_fast_slope = daily['weekly_ema_fast_slope'].iloc[begin_idx:end_idx]
        weekly_ema_fast_slope[weekly_ema_fast > weekly_ema_fast.shift(1)] = 'rising'
        weekly_ema_fast_slope[weekly_ema_fast < weekly_ema_fast.shift(1)] = 'falling'
        weekly_ema_fast_slope[weekly_ema_fast == weekly_ema_fast.shift(1)] = 'flat'

        # calulate weekly MACD line
        weekly_macd_ema_fast = daily['weekly_macd_ema_fast'].iloc[begin_idx:end_idx]
        weekly_macd_ema_slow = daily['weekly_macd_ema_slow'].iloc[begin_idx:end_idx]
        daily['weekly_macd_line'] = weekly_macd_ema_fast - weekly_macd_ema_slow
        weekly_macd_line = daily['weekly_macd_line'].iloc[begin_idx:end_idx]

        # calculate weekly MACD signal_line
        period = scenes['macd_signal_line_period'].iloc[scene_id]
        daily['weekly_signal_line'] = weekly_macd_line.ewm(span=period, min_periods=period, adjust=False).mean()
        weekly_signal_line = daily['weekly_signal_line'].iloc[begin_idx:end_idx]

        # calculate weekly MACD histogram
        daily['weekly_macd_hist'] = weekly_macd_line - weekly_signal_line
        weekly_macd_hist = daily['weekly_macd_hist'].iloc[begin_idx:end_idx]

        # calculate MACD histogram slope
        weekly_macd_hist_slope = daily['weekly_macd_hist_slope'].iloc[begin_idx:end_idx]
        weekly_macd_hist_slope[weekly_macd_hist > weekly_macd_hist.shift(1)] = 'rising'
        weekly_macd_hist_slope[weekly_macd_hist < weekly_macd_hist.shift(1)] = 'falling'
        weekly_macd_hist_slope[weekly_macd_hist == weekly_macd_hist.shift(1)] = 'flat'

         # calculate weekly Impulse Color
        weekly_impulse_color = daily['weekly_impulse_color'].iloc[begin_idx:end_idx]
        cond = weekly_ema_fast_slope + '-' + weekly_macd_hist_slope  # dirty way to concat two columns

        weekly_impulse_color[cond == 'rising-rising'] = 'green'
        weekly_impulse_color[cond == 'falling-falling'] = 'red'
        weekly_impulse_color[cond == 'falling-rising'] = 'blue'
        weekly_impulse_color[cond == 'rising-falling'] = 'blue'
        weekly_impulse_color[cond == 'rising-flat'] = 'blue'
        weekly_impulse_color[cond == 'flat-rising'] = 'blue'
        weekly_impulse_color[cond == 'flat-flat'] = 'yellow'
        

        ###############################################################################
        # run System Trading Rules
        # NOTE:
        #  Section 6.1: Trend Follow Trade
        ###############################################################################

        ################################################################
        # Trend Follow Trade
        ################################################################
        # weekly uptrend
        daily['weekly_uptrend'] = (weekly_ema_slow_slope == 'rising') & \
            ((weekly_impulse_color == 'green') | (weekly_impulse_color =='blue'))
        weekly_uptrend = daily['weekly_uptrend'].iloc[begin_idx:end_idx]

        weekly_csize = daily['weekly_csize'].iloc[begin_idx:end_idx]
        weekly_csize[start_of_week] = daily_csize.shift(1)
        weekly_csize.ffill(inplace=True)                # forward fill the remaing of the week

        # upsample weekly_uptrend to daily['weekly_trend']
        # weekly_uptrend = daily['weekly_uptrend'].iloc[begin_idx:end_idx]
        # weekly_uptrend[start_of_week] = csize.shift(1)    # for 1st day of the week, take last csize from last week
        # weekly_uptrend.ffill(inplace=True)                # forward fill the entire week (this week)

        # daily signal
        cond_m = (daily_impulse_color.shift(1) == 'red') & \
            ((daily_impulse_color == 'green') | (daily_impulse_color == 'blue'))

        cond_c1 = daily_force_index2 < 0

        cond_c2 = (daily_value_zone == 'inside') & (daily_value_zone == 'above')

        cond_c3 = daily_bullish_pin_bar | daily_bullish_engulfing | daily_piercing_pattern

        cond_c4 = daily_volume > daily_volume.shift(1)

        daily['daily_signal'] = cond_m & \
            ((cond_c1 & cond_c2) | (cond_c1 & cond_c3) | (cond_c2 & cond_c3 & cond_c4))
        daily_signal = daily['daily_signal'].iloc[begin_idx:end_idx]

        # AT-Debug code, save cond_m, cond_c1~4 into excel  
        daily['cond_m'] = cond_m
        daily['cond_c1'] = cond_c1
        daily['cond_c2'] = cond_c2
        daily['cond_c3'] = cond_c3
        daily['cond_c4'] = cond_c4

        daily['buy_signal'] = weekly_uptrend & daily_signal

        ################################################################
        # Trend Follow Trade
        ################################################################

        # Rule# 1.3.1
        rule_1_3_1 = daily_bullish_engulfing & \
            ((daily_force_index_atr_lower_ch > daily_high) | (daily_ae_lower_channel > daily_high))

        rule_1_3_2 = daily_bullish_pin_bar & \
            ((daily_force_index_atr_lower_ch > daily_high) | (daily_ae_lower_channel > daily_high))

        rule_1_3_3 = daily_piercing_pattern & \
            ((daily_force_index_atr_lower_ch > daily_high) | (daily_ae_lower_channel > daily_high))

        # Rule# 1.4
        rule_1_4 = daily_volume > daily_volume.shift(1)

        # Rule#1 Price Action Reversal Signal
        daily['price_action_reversal_signal'] = (rule_1_3_1 | rule_1_3_2 |  rule_1_3_3) & rule_1_4

        # Rule# 2 (to be reviewed)

        # Rule# 3.2
        rule_3_2 = (weekly_impulse_color.shift(1) == 'red') & \
            ((weekly_impulse_color == 'green') | (weekly_impulse_color == 'blue'))

        # Rule# 3.3: false_breakout_MACD_divergence


        # Rule# 3.5
        rule_3_5 = (daily_impulse_color != 'blue') | (daily_impulse_color != 'green')

        # Rule# 3: False Breakout with MACD Divergence
        daily['false_breakout_MACD_divergence'] = rule_3_2 & rule_3_5


        ###############################################################################
        # write to Excel files
        ###############################################################################
        ## write results to files: One Excel sheet per simulation (symbol)
        sheetname = '%s-%s-%04d' % (market, symbol.lstrip('0').zfill(5), scene_id)

        # AT-Pending: DEBUG Message
        print('\n\ntotal interations: %d in %f seconds (avg computation time: %f)' % \
            ((begin_idx-end_idx), time.perf_counter() - start, \
                (time.perf_counter() - start)/(begin_idx-end_idx)))

        # extract daily[beigin_date:end_date], and sort by ascending trade-date
        daily_subset = daily.iloc[begin_idx:end_idx]
        #daily_subset.to_excel(writer_daily, sheet_name=sheetname, freeze_panes=(1, 0), index=False)
        daily_subset.to_excel(writer_daily, sheet_name=sheetname, freeze_panes=(1, 0), index=True)

        # reverse the trade-date order
        # weekly.to_excel(writer_weekly, sheet_name=sheetname, freeze_panes=(1, 0), \
        #     index=False)

        writer_daily.save()
        # writer_weekly.save()
        exit()          # AT-Debug: let focus single simulation first    

# This is the standard bilerplate that calls that main() function.
if __name__ == "__main__":
    #main(sys.argv)
    main()
