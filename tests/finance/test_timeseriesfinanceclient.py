""" Unit tests for teii.finance.timeseries module """


import datetime
import pytest
import pandas as pd
import json

from os import remove
from teii.finance import TimeSeriesFinanceClient
from teii.finance import FinanceClientInvalidAPIKey
from teii.finance import FinanceClientInvalidData
from teii.finance import FinanceClientAPIError
from teii.finance.exception import FinanceClientParamError
from importlib import resources


def test_constructor_success(api_key_str,
                             mocked_response):
    TimeSeriesFinanceClient("IBM", api_key_str)

    

def test_constructor_failure_invalid_data(api_key_str):
    with pytest.raises(FinanceClientInvalidData):
        TimeSeriesFinanceClient("NOTICKER", api_key_str)



def test_constructor_failure_invalid_api_key():
    with pytest.raises(FinanceClientInvalidAPIKey):
        TimeSeriesFinanceClient("IBM")


def test_daily_price_no_dates(api_key_str,
                              mocked_response,
                              pandas_series_IBM_prices):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)

    ps = fc.daily_price()

    assert ps.count() == 5416   # 1999-11-01 to 2021-05-11 (5416 business days)

    assert ps.count() == pandas_series_IBM_prices.count()

    assert ps.equals(pandas_series_IBM_prices)


def test_daily_price_dates(api_key_str,
                           mocked_response,
                           pandas_series_IBM_prices_filtered):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)

    ps = fc.daily_price(datetime.date(year=2021, month=1, day=1),
                        datetime.date(year=2021, month=2, day=28),)

    assert ps.count() == 38   # 2021-01-04 to 2021-02-26 (38 business days)

    assert ps.count() == pandas_series_IBM_prices_filtered.count()

    assert ps.equals(pandas_series_IBM_prices_filtered)

    
def test_to_pandas_data_frame(api_key_str,
                              mocked_response,
                              pandas_series_IBM):
        
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        
        df = pd.DataFrame.from_dict(pandas_series_IBM, orient='index', dtype=float)
        
        df.index = df.index.astype("datetime64[ns]")
        df = df.sort_index(ascending=True)
        df[['6. volume','8. split coefficient']]=df[['6. volume','8. split coefficient']].astype('int32')
        df = df.rename(columns={'1. open': 'open','2. high': 'high',
                                '3. low':'low', '4. close':'close',
                                '5. adjusted close':'aclose',
                                '6. volume':'volume',
                                '7. dividend amount':'dividend',
                                '8. split coefficient':'splitc'})
        
        
        assert pd.testing.assert_frame_equal(df,fc.to_pandas(),
                                             check_dtype=False,
                                             check_column_type=False,
                                             check_names=False,
                                             check_frame_type=False)==None
    
    
def test_to_csv_data_frame(api_key_str,
                           mocked_response,
                           pandas_series_IBM,
                           path2file):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
    
    df = pd.DataFrame.from_dict(pandas_series_IBM, orient='index', dtype=float)
        
    df.index = df.index.astype("datetime64[ns]")
    df = df.sort_index(ascending=True)
    df[['6. volume','8. split coefficient']]=df[['6. volume','8. split coefficient']].astype('int32')
    df = df.rename(columns={'1. open': 'open','2. high': 'high',
                            '3. low':'low', '4. close':'close',
                            '5. adjusted close':'aclose',
                            '6. volume':'volume',
                            '7. dividend amount':'dividend',
                            '8. split coefficient':'splitc'})
    
    fc.to_csv(path2file)
    file = pd.read_csv(path2file, index_col=0)
    file.index = file.index.astype("datetime64[ns]")
    remove(path2file)
    
    assert pd.testing.assert_frame_equal(df,file,
                                             check_dtype=False,
                                             check_column_type=False,
                                             check_names=False,
                                             check_frame_type=False)==None

    
def test_daily_volume_no_dates(api_key_str,
                               mocked_response,
                               pandas_series_IBM_volumes):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
 
    ps = fc.daily_volume()
    
    assert ps.count() == 5416   

    assert ps.count() == pandas_series_IBM_volumes.count()['volume']

    assert pd.testing.assert_frame_equal(ps.to_frame(), pandas_series_IBM_volumes,
                                         check_dtype=False,
                                         check_column_type=False,
                                         check_names=False,
                                         check_frame_type=False)==None


def test_daily_volume_dates(api_key_str,
                            mocked_response,
                            pandas_series_IBM_volumes_filtered):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
 
    ps = fc.daily_volume(datetime.date(year=2021, month=1, day=1),
                         datetime.date(year=2021, month=2, day=28),)
    
    assert ps.count() == 38   

    assert ps.count() == pandas_series_IBM_volumes_filtered.count()['volume']

    assert pd.testing.assert_frame_equal(ps.to_frame(), pandas_series_IBM_volumes_filtered,
                                         check_dtype=False,
                                         check_column_type=False,
                                         check_names=False,
                                         check_frame_type=False)==None
    
        
def test_yearly_dividends_per_quarter_dates(api_key_str,
                            mocked_response,
                            pandas_series_IBM_dividends_per_quarter_filtered):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
 
    ps = fc.yearly_dividends_per_quarter(2000,2001)
    
    assert ps.count() == 8   

    assert ps.count() == pandas_series_IBM_dividends_per_quarter_filtered.count()['dividend']

    assert pd.testing.assert_frame_equal(ps.to_frame(), pandas_series_IBM_dividends_per_quarter_filtered,
                                         check_dtype=False,
                                         check_column_type=False,
                                         check_names=False,
                                         check_frame_type=False)==None  
       
        
def test_yearly_dividends_per_quarter_no_dates(api_key_str,
                            mocked_response,
                            pandas_series_IBM_dividends_per_quarter_unfiltered):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
 
    ps = fc.yearly_dividends_per_quarter()
    
    assert ps.count() == 87   

    assert ps.count() == pandas_series_IBM_dividends_per_quarter_unfiltered.count()['dividend']

    assert pd.testing.assert_frame_equal(ps.to_frame(), pandas_series_IBM_dividends_per_quarter_unfiltered,
                                         check_dtype=False,
                                         check_column_type=False,
                                         check_names=False,
                                         check_frame_type=False)==None  
    
    
def test_yearly_dividends_dates(api_key_str,
                            mocked_response,
                            pandas_series_IBM_dividends_filtered):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
 
    ps = fc.yearly_dividends(2000,2001)
    
    assert ps.count()['dividend'] == 2   

    assert ps.count()['dividend'] == pandas_series_IBM_dividends_filtered.count()['dividend']

    assert pd.testing.assert_frame_equal(ps, pandas_series_IBM_dividends_filtered,
                                         check_dtype=False,
                                         check_column_type=False,
                                         check_names=False,
                                         check_frame_type=False)==None  

    
def test_yearly_dividends_no_dates(api_key_str,
                            mocked_response,
                            pandas_series_IBM_dividends_unfiltered):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
 
    ps = fc.yearly_dividends()
    
    assert ps.count()['dividend'] == 23   

    assert ps.count()['dividend'] == pandas_series_IBM_dividends_unfiltered.count()['dividend']

    assert pd.testing.assert_frame_equal(ps, pandas_series_IBM_dividends_unfiltered,
                                         check_dtype=False,
                                         check_column_type=False,
                                         check_names=False,
                                         check_frame_type=False)==None    
    
    
def test_daily_price_dates_error(api_key_str,
                                  mocked_response):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
    
    with pytest.raises(FinanceClientParamError):
        fc.daily_price(datetime.date(year=2021, month=1, day=1),
                            datetime.date(year=2020, month=1, day=1),)
    
    
def test_daily_price_params_error(api_key_str,
                                  mocked_response):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
    
    with pytest.raises(FinanceClientParamError):
        fc.daily_price("Lunes",
                            datetime.date(year=2020, month=1, day=1),)        
    
    
def test_daily_volume_dates_error(api_key_str,
                                  mocked_response):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
    
    with pytest.raises(FinanceClientParamError):
        fc.daily_volume(datetime.date(year=2021, month=1, day=1),
                            datetime.date(year=2020, month=1, day=1),)
        

def test_daily_volume_params_error(api_key_str,
                                  mocked_response):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
    
    with pytest.raises(FinanceClientParamError):
        fc.daily_volume("Lunes",
                        datetime.date(year=2020, month=1, day=1),)        
        
    
def test_malformed_JSON_column_names(api_key_str,
                                     mocked_response_malformed_cnames):
    with pytest.raises(FinanceClientInvalidData):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
                        

def test_malformed_JSON_type(api_key_str,
                             mocked_response_malformed_type):
    with pytest.raises(FinanceClientInvalidData):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
               
            

def test_malformed_JSON_format(api_key_str,
                             mocked_response_malformed_format):
    with pytest.raises(FinanceClientInvalidData):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
               
            
def test_malformed_JSON_index(api_key_str,
                              mocked_response_malformed_index):
    with pytest.raises(FinanceClientInvalidData):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
                        
        
def test_connect_to_API_failure(api_key_str,
                              mocked_response_failure):
    with pytest.raises(FinanceClientAPIError):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
    
    

