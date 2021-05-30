""" Time Series Finance Client classes """


import datetime as dt
import logging
import pandas as pd

from typing import Optional, Union

from teii.finance import FinanceClientInvalidData
from teii.finance import FinanceClient
from teii.finance.exception import FinanceClientParamError



class TimeSeriesFinanceClient(FinanceClient):
    """ Wrapper around the AlphaVantage API for Time Series Daily Adjusted.

        Source:
            https://www.alphavantage.co/documentation/ (TIME_SERIES_DAILY_ADJUSTED)
    """

    _data_field2name_type = {
            "1. open":                  ("open",     "float"),
            "2. high":                  ("high",     "float"),
            "3. low":                   ("low",      "float"),
            "4. close":                 ("close",    "float"),
            "5. adjusted close":        ("aclose",   "float"),
            "6. volume":                ("volume",   "int"),
            "7. dividend amount":       ("dividend", "float"),
            "8. split coefficient":     ("splitc",   "int"),
        }
    
    

    def __init__(self, ticker: str,
                 api_key: Optional[str] = None,
                 logging_level: Union[int, str] = logging.WARNING) -> None:
        """ TimeSeriesFinanceClient constructor. """

        super().__init__(ticker, api_key, logging_level)

        self._build_data_frame()

    def _build_data_frame(self) -> None:
        """ Build Panda's DataFrame and format data. """

        # TODO: Handle conversion errors

        # Build Panda's data frame
        try:
            data_frame = pd.DataFrame.from_dict(self._json_data, orient='index', dtype=float)
        except Exception as e:
            raise  FinanceClientInvalidData("JSON malformed, unable to create DataFrame from data")
        else:
             self._logger.info(f"Data from dict in JSON converted to DataFrame")
                
        # Rename data fields
        try:
            data_frame = data_frame.rename(columns={key: name_type[0]
                                                    for key, name_type in self._data_field2name_type.items()})
        except Exception as e:
            raise  FinanceClientInvalidData("JSON columns name do not match, error while formatting")
        else:
            self._logger.info(f"Dataframe column names converted")
            
        # Set data field types
        
        try:
            data_frame = data_frame.astype(dtype={name_type[0]: name_type[1]
                                                  for key, name_type in self._data_field2name_type.items()})
        except Exception as e:
            raise  FinanceClientInvalidData("Cannot convert to expected datatypes, error while formatting")
        else:
            self._logger.info(f"Dataframe column converted to the right types")    

        # Set index type
        try:
            data_frame.index = data_frame.index.astype("datetime64[ns]")
        except Exception as e:
            raise  FinanceClientInvalidData("JSON index cannot be converted to datetime, error while formatting")
        else:
            self._logger.info(f"Dataframe index converted to datetime")
            
        # Sort data
        self._data_frame = data_frame.sort_index(ascending=True)

    def _build_base_query_url_params(self) -> str:
        """ Return base query URL parameters.

        Parameters are dependent on the query type:
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=TICKER&outputsize=full&apikey=API_KEY&data_type=json
        """

        return f"function=TIME_SERIES_DAILY_ADJUSTED&symbol={self._ticker}&outputsize=full&apikey={self._api_key}"

    def _build_query_data_key(self) -> str:
        """ Return data query key. """

        return "Time Series (Daily)"

    def _validate_query_data(self) -> None:
        """ Validate query data. """

        try:
            assert self._json_metadata["2. Symbol"] == self._ticker
        except Exception as e:
            raise FinanceClientInvalidData("Metadata field '2. Symbol' not found") from e
        else:
            self._logger.info(f"Metadata key '2. Symbol' = '{self._ticker}' found")

    def daily_price(self,
                    from_date: Optional[dt.date] = None,
                    to_date: Optional[dt.date] = None) -> pd.Series:
        """ Return daily close price from 'from_date' to 'to_date'. """

        assert self._data_frame is not None

        series = self._data_frame['close']

        # TODO: Tarea 3
        #   Comprueba el valor, tipo y secuencia de from_date/to_date y
        #   genera excepción 'FinanceClientParamError' en caso de error
        
        # FIXME: type hint error
        if from_date is not None and to_date is not None:
            
            #Comprobamos que sean tipo date
            if type(from_date) is not dt.date or type(to_date) is not dt.date:
                raise FinanceClientParamError("los argumentos deben ser fechas del tipo dt.date")
            
            #Comprobamos que from_date vaya antes de to_date
            if(from_date > to_date):
                raise FinanceClientParamError("from_date no puede ser una fecha posterior a to_date")
            
            series = series.loc[from_date:to_date]   # type: ignore

        return series

    def daily_volume(self,
                     from_date: Optional[dt.date] = None,
                     to_date: Optional[dt.date] = None) -> pd.Series:
        """ Return daily volume from 'from_date' to 'to_date'. """

        assert self._data_frame is not None

        series = self._data_frame['volume']

        # TODO: Tarea 3
        #   Comprueba el valor, tipo y secuencia de from_date/to_date y
        #   genera excepción 'FinanceClientParamError' en caso de error

        # FIXME: type hint error
        if from_date is not None and to_date is not None:
            
            #Comprobamos que sean tipo date
            if type(from_date) is not dt.date or type(to_date) is not dt.date:
                raise FinanceClientParamError("los argumentos deben ser fechas del tipo dt.date")
            
            
             #Comprobamos que from_date vaya antes de to_date
            if(from_date > to_date):
                raise FinanceClientParamError("from_date no puede ser una fecha posterior a to_date")
            
            series = series.loc[from_date:to_date]   # type: ignore

        return series

    def yearly_dividends(self,
                         from_year: Optional[int] = None,
                         to_year: Optional[int] = None) -> pd.Series:
        #""" Return yearly dividends from 'from_year' to 'to_year'. """

        # TODO: Tarea 3
        #   Implementa este método...
        
        assert self._data_frame is not None

        series = self._data_frame['dividend']
        
        if from_year is not None and to_year is not None:
            
            #Comprobamos que sean tipo int
            if type(from_year) is not int or type(to_year) is not int:
                raise FinanceClientParamError("los argumentos deben ser años del tipo int")
            
            
            #Comprobamos que from_year vaya antes de to_year
            if(from_year > to_year):
                raise FinanceClientParamError("from_date no puede ser un año posterior a to_date")
            
            from_date = dt.date(year=from_year, month=1, day=1)
            to_date = dt.date(year=to_year, month=12, day=31)
            
            series = series.loc[from_date:to_date]   # type: ignore

        series = series[series!=0]
        return series
        
        return None
    
        
    def yearly_dividends_per_quarter(self,
                                     from_year: Optional[int] = None,
                                     to_year: Optional[int] = None) -> pd.Series:
        """ Return yearly dividends per quarter from 'from_year' to 'to_year'. """

        # TODO: Tarea 3
        #   Implementa este método...

        return None
 
    