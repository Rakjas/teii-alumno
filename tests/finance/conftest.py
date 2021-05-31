""" Unit tests for teii.finance subpackage """


import json
import pandas as pd
import unittest.mock as mock
import teii.finance.finance
import datetime as dt

from importlib import resources
from pytest import fixture


@fixture(scope='session')
def api_key_str(request):
    return ("nokey")


@fixture(scope='session')
def path2file():
    return("./temp.csv")


@fixture(scope='session')
def pandas_series_IBM_highest_daily_variation():
    date = pd.to_datetime('2020-03-16')
    return[date, 107.41, 95.0, 12.409999999999997]


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


@fixture(scope='function')
def mocked_response_malformed_type():
    response = mock.Mock()
    response.status_code = 200
    with resources.open_text('teii.finance.data', 'TIME_SERIES_DAILY_MALFORMED_TYPE.IBM.json') as json_fid:
        json_data = json.load(json_fid)
    response.json.return_value = json_data

    requests = mock.Mock()
    requests.get.return_value = response

    teii.finance.finance.requests = requests
    

@fixture(scope='function')
def mocked_response_malformed_index():
    response = mock.Mock()
    response.status_code = 200
    with resources.open_text('teii.finance.data', 'TIME_SERIES_DAILY_MALFORMED_INDEX.IBM.json') as json_fid:
        json_data = json.load(json_fid)
    response.json.return_value = json_data

    requests = mock.Mock()
    requests.get.return_value = response

    teii.finance.finance.requests = requests
    



@fixture(scope='function')
def mocked_response_malformed():
    response = mock.Mock()
    response.status_code = 200
    with resources.open_text('teii.finance.data', 'TIME_SERIES_DAILY_MALFORMED_TYPE.IBM.json') as json_fid:
        json_data = json.load(json_fid)
    response.json.return_value = json_data

    requests = mock.Mock()
    requests.get.return_value = response

    teii.finance.finance.requests = requests 
    
    
@fixture(scope='function')  
def mocked_response_malformed_cnames():
    response = mock.Mock()
    response.status_code = 200
    with resources.open_text('teii.finance.data', 'TIME_SERIES_DAILY_MALFORMED_CNAMES.IBM.json') as json_fid:
        json_data = json.load(json_fid)
    response.json.return_value = json_data

    requests = mock.Mock()
    requests.get.return_value = response

    teii.finance.finance.requests = requests 
    
 
@fixture(scope='function')  
def mocked_response_malformed_format():
    response = mock.Mock()
    response.status_code = 200
    with resources.open_text('teii.finance.data', 'TIME_SERIES_DAILY_MALFORMED_FORMAT.IBM.json') as json_fid:
        json_data = json.load(json_fid)
    response.json.return_value = json_data

    requests = mock.Mock()
    requests.get.return_value = response

    teii.finance.finance.requests = requests 
    

@fixture(scope='package')
def mocked_response_failure():
    response = mock.Mock()
    response.status_code = 0
    
    requests = mock.Mock()
    requests.get.return_value = response

    teii.finance.finance.requests = requests

        
@fixture(scope='package')
def pandas_series_IBM_volumes():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.volume.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
    return df

       
@fixture(scope='package')
def pandas_series_IBM_volumes_filtered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.volume.filtered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
    return df

    
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


@fixture(scope='package')
def pandas_series_IBM():
    with resources.open_text('teii.finance.data', 'TIME_SERIES_DAILY_MOCKED.IBM.json') as json_fid:
        json_data = json.load(json_fid)
    return json_data


@fixture(scope='package')
def pandas_series_IBM_dividends_per_quarter_filtered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.dividends.per.quarter.filtered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
    return df


@fixture(scope='package')
def pandas_series_IBM_dividends_filtered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.dividends.filtered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
    return df


@fixture(scope='package')
def pandas_series_IBM_dividends_per_quarter_unfiltered():
    with resources.path('teii.finance.data',
                        'TIME_SERIES_DAILY_ADJUSTED.IBM.dividends.per.quarter.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
    return df


@fixture(scope='package')
def pandas_series_IBM_dividends_unfiltered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.dividends.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
    return df
