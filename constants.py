#!/usr/bin/python3

"""
define global constant
"""

# ONLY query MySQL records later than SYSTEM_START_TIME
SYSTEM_START_DATE = '2015-01-01'

# max numbers of daily-quotes to query from MySQL
MAX_HOLIDAY_REOCORD = 365 * 10

# max numbers of daily-quotes to query from MySQL
MAX_DAILY_REOCORD = int(365 * (5 / 7) * 10)

# max number of weekly-qoutes to query from MySQL
MAX_YEARLY_RECORD = 52 * 10

# for MySQL & analysis, define column name, type, default-value, raw/calcaulte

# Illustration
#  'symbol' : MySQL (also name of numpy dailies['symbol'] in analysis.run() )
#  'float'  : data type of the numpy column
#  0.0  : default value (with proper data type)
#  'raw/static/dynamic':
#       raw : raw data (without any processing) from 3rd party website
#       static: calculated result is INDEPENDENT of scenarios,
#               e.g. ema13 has the same result regardless scenario_id
#       dynamic: calculated result varies in different scenarios

DAILY_COLUMNS = ( \
    ('market', 'str', 'HSI', 'raw'), \
    ('symbol', 'str', 'none', 'raw'), \
    ('date', 'date', '0000-00-00', 'raw'), \
    ('open', 'float', 0.0, 'raw'), \
    ('high', 'float', 0.0, 'raw'), \
    ('low', 'float', 0.0, 'raw'), \
    ('close', 'float', 0.0, 'raw'), \
    ('volume', 'float', 0.0, 'raw'), \
    ('split_ratio', 'float', 1.0, 'raw'), \
    ('dividend', 'float', 0.0, 'raw'), \
    ('ema_fast', 'float', 0.0, 'indicator'), \
    ('ema_slow', 'float', 0.0, 'indicator'), \
    ('ema_fast_slope', 'str', '', 'indicator'), \
    ('ema_slow_slope', 'str', '', 'indicator'), \
    ('macd_line', 'float', 0.0, 'indicator'), \
    ('signal_line', 'float', 0.0, 'indicator'), \
    ('macd_hist', 'float', 0.0, 'indicator'), \
    ('macd_hist_slope', 'str', '', 'indicator'), \
    ('impulse_color', 'str', '', 'indicator'), \
    ('force_index', 'float', 0.0, 'indicator'), \
    ('force_index2', 'float', 0.0, 'indicator'), \
    ('force_index13', 'float', 0.0, 'indicator'), \
    ('true_range', 'float', 0.0, 'indicator'), \
    ('atr', 'float', 0.0, 'indicator'), \
    ('value_zone', 'str', '', 'indicator'), \
    ('bearish_engulfing', 'bool', False, 'indicator'), \
    ('bullish_engulfing', 'bool', False, 'indicator'), \
    ('bearish_pin_bar', 'bool', False, 'indicator'), \
    ('bullish_pin_bar', 'bool', False, 'indicator'), \
    ('piercing_pattern', 'bool', False, 'indicator'), \
    ('dark_cloud', 'bool', False, 'indicator'), \
    ('csize', 'float', 0.0, 'indicator'), \
    ('weekly_csize', 'float', 0.0, 'indicator'),\
    ('ae_upper_channel', 'float', 0.0, 'indicator'), \
    ('ae_lower_channel', 'float', 0.0, 'indicator'), \
    ('force_index_atr_upper_ch', 'float', 0.0, 'indicator'), \
    ('force_index_atr_lower_ch', 'float', 0.0, 'indicator'), \
    ('weekly_close', 'float', [], 'indicator'), \
    ('weekly_ema_fast', 'float', 0.0, 'indicator'), \
    ('weekly_ema_slow', 'float', 0.0, 'indicator'), \
    ('weekly_macd_ema_fast', 'float', 0.0, 'indicator'), \
    ('weekly_macd_ema_slow', 'float', 0.0, 'indicator'), \
    ('weekly_ema_fast_slope', 'str', '', 'indicator'), \
    ('weekly_ema_slow_slope', 'str', '', 'indicator'), \
    ('weekly_macd_line', 'float', 0.0, 'indicator'), \
    ('weekly_signal_line', 'float', 0.0, 'indicator'), \
    ('weekly_macd_hist', 'float', 0.0, 'indicator'), \
    ('weekly_macd_hist_slope', 'str', '', 'indicator'), \
    ('weekly_impulse_color', 'str', '', 'indicator'), \
    ('weekly_uptrend', 'float', 0.0, 'indicator'),\
    ('stochastic', 'float', 0.0, 'indicator'), \
    ('rsi', 'float', 0.0, 'indicator'), \
    ('cci', 'float', 0.0, 'indicator'), \
    ('uptrendline', 'float', 0.0, 'indicator'), \
    ('downtrendline', 'float', 0.0, 'indicator'), \
    ('trendline_angle', 'float', 0.0, 'indicator'), \
        # 60-degree tend to lead to major reversal
    ('support_line', 'float', 0.0, 'indicator'), \
    ('resistance_line', 'float', 0.0, 'indicator'), \

    ('buy_signal', 'bool', False, 'decision'), \
    ('price_action_reversal_signal', 'bool', False, 'decision'), \
    ('false_breakout_MACD_divergence', 'bool', False, 'decision'), \

### DEBUG ONLY
    ('max_diff', 'float', 0.0, 'indicator'), \
    ('channel', 'float', 0.0, 'indicator'), \
    ('cond_m', 'bool', False, 'decision'), \
    ('cond_c1', 'bool', False, 'decision'), \
    ('cond_c2', 'bool', False, 'decision'), \
    ('cond_c3', 'bool', False, 'decision'), \
    ('cond_c4', 'bool', False, 'decision'), \
    ('daily_signal', 'bool', False, 'decision'), \

    # ('ema12', 'float', 0.0, 'indicator'), \
    # ('ema26', 'float', 0.0, 'indicator'), \
    # ('ema11_slope', 'str', 'invalid', 'static'), \
    # ('ema22_slope', 'str', 'invalid', 'static'), \
    )
"""
    ('ema_fast', 'float', 'nan', 'dynamic'), \
    ('ema_slow', 'float', 'nan', 'dynamic'), \
    ('cci', 'float', 'nan, 'dynamic'), \
    ('rsi', 'float', 'nan, 'dynamic'), \

"""
PERFORMANCE_COLUMNS = (\
    ('tbd', 'int', 0), \
    )

SCENARIO_COLUMNS = (\
    ('id', 'int', 0), \
    ('atr_period', 'int', 14), \
    ('daily_ema_fast', 'int', 11), \
    ('daily_ema_slow', 'int', 22), \
    ('weekly_ema_fast', 'int', 13), \
    ('weekly_ema_slow', 'int', 26), \
    ('macd_fast_period', 'int', 12), \
    ('macd_slow_period', 'int', 26), \
    ('macd_signal_line_period', 'int', 9), \
    ('bearish_pin_bar', 'list', (4, 0.001, 0.75)), \
    ('bullish_pin_bar', 'list', (3, 0.001, 0.60)), \
    ('piercing_pattern', 'list', (2, 0.001, 0.60)), \
    ('dark_cloud', 'list', (2, 0.001, 0.60)), \
    ('csize_factor', 'int', 27), \
    ('csize_window', 'int', 100), \
    ('fi_atr_lower_channel_multiplier', 'int', 3), \
    ('fi_atr_upper_channel_multiplier', 'int', 3), \
    ('slow_stochastic_oscillator', 'list', (0, 6, 6, 20, 80)), \
    ('daily_stochastic_oscillator', 'list', (14, 3)), \
    ('cci_length', 'int', 14), \
    ('cci_oversold', 'int', -100), \
    ('cci_overbough', 'int', 100), \
    ('rsi_period', 'int', 14), \
    ('rsi_oversold', 'int', 30), \
    ('rsi_overbought', 'int', 70), \
    ('bband_peroid', 'int', 20), \
    ('bband_stdev', 'float', 2.00), \
    ('bband_avg_type', 'string', 1.0), \
    ('daily_force_index_atr_low_ch_multiplier', 'float', 3.0), \
    )

TRADE_COLUMNS = (\
    ('simulation_id', 'int', 0), \
    ('scenario_id', 'int', 0), \
    ('market', 'str', 'HSI', 'raw'), \
    ('symbol', 'int', 'none'), \
    ('buy_date', 'date', '0000-00-00'), \
    ('buy_low', 'float', 0.0), \
    ('buy_price', 'float', 0.0), \
    ('stop_loss', 'float', 0.0), \
    ('sell_date', 'date', '0000-00-00'), \
    ('sell_price', 'float', 0.0), \
    ('profit', 'float', 0.0), \
    ('days_held', 'int', 0), \
    )

WEEKLY_COLUMNS = ( \
    ('market', 'str', 'HSI', 'raw'), \
    ('symbol', 'str', 'none'), \
    ('year', 'int', 2000), \
    ('week', 'int', 0), \
    #('yr_wk', 'str', ''), \
    ('open', 'float', 0.0), \
    # ('high', 'float', 0.0), \
    # ('low', 'float', 0.0), \
    ('close', 'float', 0.0), \
    ('first_dow', 'int', 0), \
    ('last_dow', 'int', 0), \
    ('first_day', 'date', '0000-00-00'), \
    ('last_day', 'date', '0000-00-00'), \
    ('ema_fast', 'float', 0.0), \
    ('ema_slow', 'float', 0.0), \
    ('ema_fast_slope', 'str', ''), \
    ('ema_slow_slope', 'str', ''), \
    ('macd_line', 'float', 0.0), \
    ('signal_line', 'float', 0.0), \
    ('macd_hist', 'float', 0.0), \
    ('macd_hist_slope', 'str', ''), \
    ('impulse_color', 'str', ''),  \
    ('uptrend', 'bool', False), \
    ('stochastic', 'float', 0.0), \

    ### DEBUG ONLY
    ('cond_m', 'bool', False), \
    ('cond_c1', 'bool', False), \
    ('cond_c2', 'bool', False), \
    ('cond_c3', 'bool', False), \
    ('cond_c4', 'bool', False), \
    )
