#!/usr/bin/python3

"""
proprietary indicators
"""

# standard imports
##import logging
import random
import statistics

# third party imports
import numpy as np

# local application imports
import constants
from database import write_log

# define "name" in log file
# logger = logging.getLogger(__name__)

import constants


def is_bearish_engulfing(close_series, open_series):
    """
    determine if bearish_engulfing

    Args:
        param1(pandas series): (daily) close series
        param4(pandas series): (daily) open series

    Returns:
        pandas series: result
    """
    bearish_engulfing = bool(random.randrange(10) < 5)
    
    return bearish_engulfing


def is_bullish_engulfing(close_series, open_series):
    """
    determine if bullish_engulfing

    Args:
        param1(pandas series): (daily) close series
        param4(pandas series): (daily) open series

    Returns:
        pandas series: result
    """
    bullish_engulfing = bool(random.randrange(10) < 5)

    return bullish_engulfing


def is_bearish_pin_bar(quotes, scene):
    """
    determine if bearish_pin_bar

    Args:
        param1(pandas dataframe): (daily) dataframe
        param2(pandas row): scenarios (row data)

    Returns:
        pandas series: result
    """
    open_series = quotes['open']
    high_series = quotes['high']
    low_series = quotes['low']
    close_series = quotes['close']
    f1, f2, f3 = scene['bearish_pin_bar']
    bearish_pin_bar = bool(random.randrange(10) < 5)

    return bearish_pin_bar


def is_bullish_pin_bar(quotes, scene):
    """
    determine if_bullish_pin_bar

    Args:
        param1(pandas dataframe): (daily) dataframe
        param2(pandas row): scenarios (row data)

    Returns:
        pandas series: result
    """
    open_series = quotes['open']
    high_series = quotes['high']
    low_series = quotes['low']
    close_series = quotes['close']
    f1, f2, f3 = scene['bullish_pin_bar']
    bullish_pin_bar = bool(random.randrange(10) < 5)

    return bullish_pin_bar


def is_dark_cloud(quotes, scene):
    """
    determine is_dark_cloud

    Args:
        param1(pandas dataframe): (daily) dataframe
        param2(pandas row): scenarios (row data)

    Returns:
        pandas series: result
    """
    open_series = quotes['open']
    high_series = quotes['high']
    low_series = quotes['low']
    close_series = quotes['close']
    f1, f2, f3 = scene['dark_cloud']
    dark_cloud = bool(random.randrange(10) < 5)

    return dark_cloud


def is_piercing_pattern(quotes, scene):
    """
    determine is_piercing_pattern

    Args:
        param1(pandas dataframe): (daily) dataframe
        param2(pandas row): scenarios (row data)

    Returns:
        pandas series: result
    """
    open_series = quotes['open']
    high_series = quotes['high']
    low_series = quotes['low']
    close_series = quotes['close']
    f1, f2, f3 = scene['piercing_pattern']
    piercing_pattern = bool(random.randrange(10) < 5)

    return piercing_pattern


