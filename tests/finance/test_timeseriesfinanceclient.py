""" Unit tests for teii.finance.timeseries module """


import datetime
import pytest

from teii.finance import TimeSeriesFinanceClient
from teii.finance import FinanceClientInvalidAPIKey
from teii.finance import FinanceClientInvalidData


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


def test_daily_volume_no_dates(api_key_str,
                               mocked_response):
    # TODO: Tarea 3
    pass


def test_daily_volume_dates(api_key_str,
                            mocked_response):
    # TODO: Tarea 3
    pass
