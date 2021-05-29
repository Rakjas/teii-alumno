""" Unit tests for teii.finance subpackage """


import json
import pandas as pd
import unittest.mock as mock
import teii.finance.finance

from importlib import resources
from pytest import fixture


@fixture(scope='session')
def api_key_str(request):
    return ("nokey")


@fixture(scope='package')
def mocked_response():
    response = mock.Mock()
    response.status_code = 200
    with resources.open_text('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.json') as json_fid:
        json_data = json.load(json_fid)
    response.json.return_value = json_data

    requests = mock.Mock()
    requests.get.return_value = response

    teii.finance.finance.requests = requests


@fixture(scope='package')
def pandas_series_IBM_prices():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.prices.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['close']
    return ds


@fixture(scope='package')
def pandas_series_IBM_prices_filtered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.prices.filtered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['close']
    return ds
