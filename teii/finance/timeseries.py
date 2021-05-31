""" Time Series Finance Client classes """


import datetime as dt
import logging
import pandas as pd
from calendar import monthrange

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
            raise FinanceClientInvalidData({"JSON malformed, unable to create DataFrame from data"}) from e
        else:
            self._logger.info({"Data from dict in JSON converted to DataFrame"})

        # Rename data fields
        try:
            data_frame = data_frame.rename(columns={key: name_type[0]
                                                    for key, name_type in self._data_field2name_type.items()})
        except Exception as e:
            raise FinanceClientInvalidData({"JSON columns name do not match, error while formatting"}) from e
        else:
            self._logger.info({"Dataframe column names converted"})

        # Set data field types

        try:
            data_frame = data_frame.astype(dtype={name_type[0]: name_type[1]
                                                  for key, name_type in self._data_field2name_type.items()})
        except Exception as e:
            raise FinanceClientInvalidData("Cannot convert to expected datatypes, error while formatting") from e
        else:
            self._logger.info({"Dataframe column converted to the right types"})

        # Set index type
        try:
            data_frame.index = data_frame.index.astype("datetime64[ns]")
        except Exception as e:
            raise FinanceClientInvalidData("JSON index cannot be converted to datetime, error while formatting") from e
        else:
            self._logger.info({"Dataframe index converted to datetime"})

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

            # Comprobamos que sean tipo date
            if type(from_date) is not dt.date or type(to_date) is not dt.date:
                raise FinanceClientParamError("los argumentos deben ser fechas del tipo dt.date")

            # Comprobamos que from_date vaya antes de to_date
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

            # Comprobamos que sean tipo date
            if type(from_date) is not dt.date or type(to_date) is not dt.date:
                raise FinanceClientParamError("los argumentos deben ser fechas del tipo dt.date")

            # Comprobamos que from_date vaya antes de to_date
            if(from_date > to_date):
                raise FinanceClientParamError("from_date no puede ser una fecha posterior a to_date")

            series = series.loc[from_date:to_date]   # type: ignore

        return series

    def yearly_dividends(self,
                         from_year: Optional[int] = None,
                         to_year: Optional[int] = None) -> pd.Series:
        """ Return yearly dividends from 'from_year' to 'to_year'. """

        # TODO: Tarea 3
        #   Implementa este método...

        assert self._data_frame is not None

        serie = pd.DataFrame(columns=('dividend',))

        if from_year is not None and to_year is not None:

            # Comprobamos que sean tipo int
            if type(from_year) is not int or type(to_year) is not int:
                raise FinanceClientParamError("los argumentos deben ser años del tipo int")

            # Comprobamos que from_year vaya antes de to_year
            if(from_year > to_year):
                raise FinanceClientParamError("from_date no puede ser un año posterior a to_date")

            # Sacamos y calculamos el valor anual para la serie a devolver
            for i in range(from_year, to_year+1):

                series = self._data_frame['dividend']

                from_date = dt.date(year=i, month=1, day=1)
                to_date = dt.date(year=i, month=12, day=31)

                series = series.loc[from_date:to_date]

                series = series[series != 0]

                total = 0

                for value in series:

                    total = total + value

                serie.loc[from_date] = [total]

        # no especificamos dates
        else:

            for i in range(self._data_frame['dividend'].head(1).index.year.values.astype(int)[0],
                           self._data_frame['dividend'].tail(1).index.year.values.astype(int)[0] + 1):

                series = self._data_frame['dividend']

                from_date = dt.date(year=i, month=1, day=1)
                to_date = dt.date(year=i, month=12, day=31)

                series = series.loc[from_date:to_date]

                series = series[series != 0]
                total = 0

                for value in series:

                    total = total + value

                serie.loc[from_date] = [total]

        serie.index = serie.index.astype("datetime64[ns]")

        return serie

    def yearly_dividends_per_quarter(self,
                                     from_year: Optional[int] = None,
                                     to_year: Optional[int] = None) -> pd.Series:
        """ Return yearly dividends per quarter from 'from_year' to 'to_year'. """
        # TODO: Tarea 3
        #   Implementa este método...
        assert self._data_frame is not None

        series = self._data_frame['dividend']

        if from_year is not None and to_year is not None:

            # Comprobamos que sean tipo int
            if type(from_year) is not int or type(to_year) is not int:
                raise FinanceClientParamError("los argumentos deben ser años del tipo int")

            # Comprobamos que from_year vaya antes de to_year
            if(from_year > to_year):
                raise FinanceClientParamError("from_date no puede ser un año posterior a to_date")

            from_date = dt.date(year=from_year, month=1, day=1)
            to_date = dt.date(year=to_year, month=12, day=31)

            series = series.loc[from_date:to_date]   # type: ignore

        series = series[series != 0]

        return series

    def highest_daily_variation(self) -> [dt.date, float, float, float]:
        """Return date where the diference between high and low was the highest of the ticker"""

        assert self._data_frame is not None

        high = self._data_frame['high']
        low = self._data_frame['low']

        index = 0
        maxdiff = 0
        i = 0
        for h, l in zip(high, low):

            diff = h - l

            if diff > maxdiff:
                maxdiff = diff
                index = i
            i = i + 1

        return [high.index.values[index].astype("datetime64[ns]"),
                high.values[index],
                low.values[index],
                maxdiff]

    def highest_monthly_mean_variation(self) -> [dt.date, float]:
        """Return month where the mean of the diference between high and low was the highest of the ticker"""

        assert self._data_frame is not None

        index = 0
        maxmean = 0
        aux = 0

        # recorremos anualmente los valores
        for i in range(self._data_frame.head(1).index.year.values.astype(int)[0],
                       self._data_frame.tail(1).index.year.values.astype(int)[0] + 1):
            # recorremos mes a mes del año
            for j in range(1, 13):

                high = self._data_frame['high']
                low = self._data_frame['low']

                from_date = dt.date(year=i, month=j, day=1)
                to_date = dt.date(year=i, month=j, day=monthrange(i, j)[1])

                high = high.loc[from_date:to_date]
                low = low.loc[from_date:to_date]

                diffparcial = 0
                dias = 0
                # recorremos dia a dia para sumar las diferencias
                for h, l in zip(high, low):

                    diff = h - l
                    diffparcial = diffparcial + diff
                    aux = aux + 1
                    dias = dias + 1

                # calculamos la media del mes
                if dias != 0:
                    mean = diffparcial/dias

                    # si es la mayor hasta la fecha la guardamos junto con su indice
                    if mean > maxmean:
                        maxmean = mean
                        index = aux-1

        high = self._data_frame['high']
        low = self._data_frame['low']
        years = high.index.values[index].astype('datetime64[Y]').astype(int) + 1970
        months = high.index.values[index].astype('datetime64[M]').astype(int) % 12 + 1

        date = dt.date(year=years, month=months, day=1)

        return [date, maxmean]
