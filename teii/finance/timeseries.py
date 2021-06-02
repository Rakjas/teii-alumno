""" Time Series Finance Client classes """


import datetime as dt
import logging
import pandas as pd  # type: ignore
from calendar import monthrange

from typing import Optional, Union

from teii.finance import FinanceClientInvalidData
from teii.finance import FinanceClient
from teii.finance.exception import FinanceClientParamError


class TimeSeriesFinanceClient(FinanceClient):
    """ Wrapper around the AlphaVantage API for Time Series Daily Adjusted.
        Source:
            https://www.alphavantage.co/documentation/ (TIME_SERIES_DAILY_ADJUSTED)

    Parameters
    ----------
    ticker : list of str
        List of tickers wich will be used to query data from alpha vantage, each of
        them will be answered with a json with financial data of the client represented
        by that ticker (for example AMZN -> Amazon)

    api_key : [optional] str, default = None
        Api_key that will be used to request the data in the API

    logging_level : [optional] Union of int and str, default logging.WARNING
        Level of info that will be written by the logger in a log file

    Attributes
    ----------
    _data_field2name_type : dict of tuples of str
        Contains traductions to generate the data frame columns names and their types
        Will be used several times when building pandas dataframes from the json data.

    _data_frame : list of pandas.dataframes
        Contains all the data from the tickers obtained from queries to de API after
        being procesed and given the right format

    _logger : object
        Reference to the object that will be used to generate the log file

    _json_data : list of dict
        Where all the info obtained from the queries to the API will be stored
        before being processed and transformed into a DataFrame

    Methods
    -------

    __init__(ticker, api_key, logging_level)
        Constructor for TimeSeriesFinanceClient

    daily_price(from_date: Optional[dt.date] = None,
                to_date: Optional[dt.date] = None)
        Return daily close price from 'from_date' to 'to_date, or if no date are specified
        daily close price from all the data avaliable.

    daily_volume(from_date: Optional[dt.date] = None,
                 to_date: Optional[dt.date] = None)
        Return daily volume from 'from_date' to 'to_date', or if no date are specified
        daily volume from all the data avaliable.

    yearly_dividends(from_year: Optional[int] = None,
                     to_year: Optional[int] = None)
        Return yearly dividends from 'from_year' to 'to_year', or if no dates are specified
        dividends from all the data avaliable by years.

    yearly_dividends_per_quarter(from_year: Optional[int] = None,
                                 to_year: Optional[int] = None)
        Return yearly dividends per quarter from 'from_year' to 'to_year', or if no dates
        are specified dividens from all the data avaliable by quarters.

    highest_daily_variation()
        Return date where the diference between high and low was the highest of the ticker

    highest_monthly_mean_variation()
        Return month where the mean of the diference between high and low was the highest of the ticker

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

    def __init__(self, ticker: list,
                 api_key: Optional[str] = None,
                 logging_level: Union[int, str] = logging.WARNING) -> None:
        """ TimeSeriesFinanceClient constructor.

        Parameters
        ----------

    ticker : list of str
        List of tickers wich will be used to query data from alpha vantage, each of
        them will be answered with a json with financial data of the client represented
        by that ticker (for example AMZN -> Amazon)

    api_key : [optional] str, default = None
        Api_key that will be used to request the data in the API

    logging_level : [optional] Union of int and str, default logging.WARNING
        Level of info that will be written by the logger in a log file

        Notes
        -----
        This class is actually a subclass from the abstract class 'FinanceClient'.
        This constructor relies on father's constructor and only implements few
        parts on _build_base_query_url_params(), _build_query_data_key(),
        _build_data_frame and _validate_query_data(self).

        """

        super().__init__(ticker, api_key, logging_level)

        self._build_data_frame()

    def _build_data_frame(self) -> None:
        """ Build Panda's DataFrame and format data.

        Notes
        -----
        Part of the constructor function, will be called at the end to store in _data_frame
        a list with all the dataframes obtained from queries to the API using tickers

        """

        # TODO: Handle conversion errors

        # Build Panda's data frame
        self._data_frame = list()
        for jData in self._json_data:
            try:
                data = pd.DataFrame.from_dict(jData, orient='index', dtype=float)
            except Exception as e:
                raise FinanceClientInvalidData({"JSON malformed, unable to create DataFrame from data"}) from e
            else:
                self._logger.info({"Data from dict in JSON converted to DataFrame"})

            # Rename data fields
            try:
                data = data.rename(columns={key: name_type[0]
                                            for key, name_type in self._data_field2name_type.items()})
            except Exception as e:
                raise FinanceClientInvalidData({"JSON columns name do not match, error while formatting"}) from e
            else:
                self._logger.info({"Dataframe column names converted"})

            # Set data field types

            try:
                data = data.astype(dtype={name_type[0]: name_type[1]
                                          for key, name_type in self._data_field2name_type.items()})
            except Exception as e:
                raise FinanceClientInvalidData("Cannot convert to expected datatypes, error while formatting") from e
            else:
                self._logger.info({"Dataframe column converted to the right types"})

            # Set index type
            try:
                data.index = data.index.astype("datetime64[ns]")
            except Exception as e:
                raise FinanceClientInvalidData("JSON index cannot be converted to datetime, error in format") from e
            else:
                self._logger.info({"Dataframe index converted to datetime"})

            # Sort data
            self._data_frame.append(data.sort_index(ascending=True))
        self._logger.info({"Collected all data frames"})

    def _build_base_query_url_params(self) -> list:
        """ Return base query URL parameters.

        Parameters are dependent on the query type:
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=TICKER&outputsize=full&apikey=API_KEY&data_type=json
        """
        response = list()
        self._logger.info(f"Building response from{self._ticker}")
        for tick in self._ticker:
            self._logger.info(f"Iterating{tick}")
            response.append(f"function=TIME_SERIES_DAILY_ADJUSTED&symbol={tick}&outputsize=full&apikey={self._api_key}")
        return response

    def _build_query_data_key(self) -> str:
        """ Return data query key. """

        return "Time Series (Daily)"

    def _validate_query_data(self) -> None:
        """ Validate query data.

        Notes
        -----
        Auxiliar function that checks the format of the obtained data and metadata
        Used by the constructor.

        """

        for meta, tick in zip(self._json_metadata, self._ticker):
            try:
                assert meta["2. Symbol"] == tick
            except Exception as e:
                raise FinanceClientInvalidData("Metadata field '2. Symbol' not found") from e
            else:
                self._logger.info(f"Metadata key '2. Symbol' = '{tick}' found")

    def daily_price(self,
                    from_date: Optional[dt.date] = None,
                    to_date: Optional[dt.date] = None) -> list:
        """
        Return daily close price from 'from_date' to 'to_date, or if no date are specified
        daily close price from all the data avaliable.

        Parameters
        ----------
    from_date : [optional] datetime.date
        Starting date used to filter the data.

    to_date : [optional] datetime.date
        Last date that will be filtered from data.

        Returns
        -------
        List of dataframes, one for each ticker used to build the Financial client.
        All of them filled with only the column 'close' that represents close price.

        Other Parameters
        ----------------
    response : list of dataframes
        Auxiliar variable that store the diferent data filtered, as it is
        generated on different loops and we need to merge it together to return
        it at the end.

    series : pandas.DataFrame
        Dataframe containing the data from the list of dataframes stored on _data_frame
        that will be looped throught.
        On each main iteration represents the data from each ticker used to build the
        Finance client object created.

        """
        response = list()
        self._logger.info("Starting daily_price query")
        for data in self._data_frame:

            assert data is not None

            series = data['close']
            self._logger.info("Extracting data from dataframe")
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

                self._logger.info("Dates in daily_price are correct")
                series = series.loc[from_date:to_date]   # type: ignore
            response.append(series)
        return response

    def daily_volume(self,
                     from_date: Optional[dt.date] = None,
                     to_date: Optional[dt.date] = None) -> list:
        """
        Return daily volume from 'from_date' to 'to_date', or if no date are specified
        daily volume from all the data avaliable.

        Parameters
        ----------
    from_date : [optional] datetime.date
        Starting date used to filter the data.

    to_date : [optional] datetime.date
        Last date that will be filtered from data.

        Returns
        -------
        List of dataframes, one for each ticker used to build the Financial client.
        All of them filled with only the column 'volume' that represents the volume
        obtained by the company.

        Other Parameters
        ----------------
    response : list of dataframes
        Auxiliar variable that store the diferent data filtered, as it is
        generated on different loops and we need to merge it together to return
        it at the end.

    series : pandas.DataFrame
        Dataframe containing the data from the list of dataframes stored on _data_frame
        that will be looped throught.
        On each main iteration represents the data from each ticker used to build the
        Finance client object created.

        """
        response = list()
        self._logger.info("Starting daily_volume query")
        for data in self._data_frame:
            assert data is not None

            series = data['volume']
            self._logger.info("Extracting data from dataframe")
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
                self._logger.info("Dates in daily_volume are correct")
                series = series.loc[from_date:to_date]   # type: ignore
            response.append(series)
        return response

    def yearly_dividends(self,
                         from_year: Optional[int] = None,
                         to_year: Optional[int] = None) -> list:
        """
        Return yearly dividends from 'from_year' to 'to_year', or if no dates are specified
        dividends from all the data avaliable by years.

        Parameters
        ----------
    from_year : [optional] int
        Starting year used to filter the data.

    to_year : [optional] int
        Last year that will be filtered from data.

        Returns
        -------
        List of dataframes, one for each ticker used to build the Financial client.
        All of them filled with only the column 'divinded' that represents the dividend
        obtained throught the year summed up.

        Other Parameters
        ----------------
    response : list of dataframes
        Auxiliar variable that store the diferent data filtered, as it is
        generated on different loops and we need to merge it together to return
        it at the end.

    from_date : datetime.date
        Starting date used to filter the data and build using the from_year variable.

    to_date : datetime.date
        Last date that will be filtered from data and build using the to_year variable.

    serie : pandas.DataFrame
        Empty DataFrame where the dividend will be stored on loops and in the end returned.

    series : pandas.DataFrame
        Dataframe containing the data from the list of dataframes stored on _data_frame
        that will be looped throught to make the calculations that will be added to serie.
        On each main iteration represents the data from each ticker used to build the
        Finance client object created.

    total : int
        Auxiliar variable used to store the summatory of dividends.

        """
        self._logger.info("Starting yearly_dividend query")
        # TODO: Tarea 3
        #   Implementa este método...
        response = list()

        for data in self._data_frame:
            assert data is not None

            serie = pd.DataFrame(columns=('dividend',))
            self._logger.info("Extracting data from dataframe")
            if from_year is not None and to_year is not None:

                # Comprobamos que sean tipo int
                if type(from_year) is not int or type(to_year) is not int:
                    raise FinanceClientParamError("los argumentos deben ser años del tipo int")

                # Comprobamos que from_year vaya antes de to_year
                if(from_year > to_year):
                    raise FinanceClientParamError("from_date no puede ser un año posterior a to_date")
                self._logger.info("Dates in yearly_dividend are correct")
                # Sacamos y calculamos el valor anual para la serie a devolver
                for i in range(from_year, to_year+1):

                    series = data['dividend']

                    from_date = series.index.get_loc(pd.to_datetime(f"1/1/{i}"), method='bfill')
                    to_date = series.index.get_loc(pd.to_datetime(f"31/12/{i}"), method='ffill')

                    series = series.iloc[from_date:to_date]

                    series = series[series != 0]

                    total = 0

                    for value in series:

                        total = total + value

                    serie.loc[pd.to_datetime(f"1/1/{i}")] = [total]

            # no especificamos dates
            else:

                for i in range(data['dividend'].head(1).index.year.values.astype(int)[0],
                               data['dividend'].tail(1).index.year.values.astype(int)[0] + 1):

                    series = data['dividend']
                    self._logger.info("Extracting data from dataframe")
                    from_date = series.index.get_loc(pd.to_datetime(f"1/1/{i}"), method='bfill')
                    to_date = series.index.get_loc(pd.to_datetime(f"31/12/{i}"), method='ffill')

                    series = series.iloc[from_date:to_date]

                    series = series[series != 0]
                    total = 0

                    for value in series:

                        total = total + value

                    serie.loc[pd.to_datetime(f"1/1/{i}")] = [total]

            serie.index = serie.index.astype("datetime64[ns]")
            response.append(serie)

        return response

    def yearly_dividends_per_quarter(self,
                                     from_year: Optional[int] = None,
                                     to_year: Optional[int] = None) -> list:
        """
        Return yearly dividends per quarter from 'from_year' to 'to_year', or if no dates
        are specified dividens from all the data avaliable by quarters.

        Parameters
        ----------
    from_year : [optional] int
        Starting year used to filter the data.

    to_year : [optional] int
        Last year that will be filtered from data.

        Returns
        -------
        List of dataframes, one for each ticker used to build the Financial client.
        All of them filled with only the column 'dividend' that represents the dividend obtained
        on each quarter.

        Other Parameters
        ----------------
    response : list of dataframes
        Auxiliar variable that store the diferent data filtered, as it is
        generated on different loops and we need to merge it together to return
        it at the end.

    series : pandas.DataFrame
        Dataframe containing the data from the list of dataframes stored on _data_frame
        that will be looped throught.
        On each main iteration represents the data from each ticker used to build the
        Finance client object created.

        """
        # TODO: Tarea 3
        #   Implementa este método...
        self._logger.info("Starting yearly_dividend query")
        response = list()
        for data in self._data_frame:
            assert data is not None

            series = data['dividend']
            self._logger.info("Extracting data from dataframe")
            if from_year is not None and to_year is not None:

                # Comprobamos que sean tipo int
                if type(from_year) is not int or type(to_year) is not int:
                    raise FinanceClientParamError("los argumentos deben ser años del tipo int")

                # Comprobamos que from_year vaya antes de to_year
                if(from_year > to_year):
                    raise FinanceClientParamError("from_date no puede ser un año posterior a to_date")

                self._logger.info("Dates in yearly_dividend are correct")
                from_date = dt.date(year=from_year, month=1, day=1)
                to_date = dt.date(year=to_year, month=12, day=31)

                series = series.loc[from_date:to_date]   # type: ignore

            series = series[series != 0]
            response.append(series)

        return response

    def highest_daily_variation(self) -> list:
        """
        Return date where the diference between high and low was the highest of the ticker

        Returns
        -------
        A list of lists [date, maxdiff, high, low], containing the max diference between the
        columns high and low daily that we have found, the date on wich it happens, and
        both values 'high' and 'low' that creates that diference on each ticker used to
        build the FinanceClient.

        Other Parameters
        ----------------
    response : list of dataframes
        Auxiliar variable that store the diferent data filtered, as it is
        generated on different loops and we need to merge it together to return
        it at the end.

    data : pandas.DataFrame
        Dataframe containing the data from the list of dataframes stored on _data_frame
        that will be looped throught.
        On each main iteration represents the data from each ticker used to build the
        Finance client object created.

    high : pandas.DataFrame
        Dataframe containing only the 'high' column of the original, used to calculate
        the diference and to obtain the max value that is also required.

    low : pandas.DataFrame
        Dataframe containing only the 'low' column of the original, used to calculate
        the diference and to obtain the low value that is also required.

    index : int
        Auxiliar value that stores the maximum diff index found at each loop, its needed
        to obtain the final values once the search (loop) ends.

    maxdiff : int
        Auxiliar value that stores the maximum diff value found at each loop, its needed
        to compare with the new diff values that we found each loop, and check if we have
        found a new maximum diference.

    i : int
        Auxiliar value that iterates with the loop to store its value on 'index' variable
        each time 'maxdiff' finds a new maximum diference value, it counts the number of loops.

        """
        response = list()
        self._logger.info("Starting highest_daily_variation query")
        for data in self._data_frame:
            assert data is not None
            self._logger.info("Extracting data from dataframe")
            high = data['high']
            low = data['low']

            index = 0
            maxdiff = 0
            i = 0
            for h, l in zip(high, low):

                diff = h - l

                if diff > maxdiff:
                    maxdiff = diff
                    index = i
                i = i + 1

            response.append([high.index.values[index].astype("datetime64[ns]"),
                             high.values[index],
                             low.values[index],
                             maxdiff])
        return response

    def highest_monthly_mean_variation(self) -> list:
        """
        Return month where the mean of the diference between high and low was the highest of the ticker

        Returns
        -------
        A list of lists [date, mean of maxdiff], containing the mean value of the diference high-low
        in the month with max diference between the columns high and low daily that we have found and the
        date on wich it happens, on each ticker used to build the FinanceClient.

        Other Parameters
        ----------------
    response : list of dataframes
        Auxiliar variable that store the diferent data filtered, as it is
        generated on different loops and we need to merge it together to return
        it at the end

    data : pandas.DataFrame
        Dataframe containing the data from the list of dataframes stored on _data_frame
        that will be looped throught.
        On each main iteration represents the data from each ticker used to build the
        Finance client object created

    high : pandas.DataFrame
        Dataframe containing only the 'high' column of the original, used to calculate
        the diference and to obtain the max value that is also required

    low : pandas.DataFrame
        Dataframe containing only the 'low' column of the original, used to calculate
        the diference and to obtain the low value that is also required

    index : int
        Auxiliar value that stores the maximum diff index found at each loop, its needed
        to obtain the final values once the search (loop) ends.

    maxmean : int
        Auxiliar value that stores the maximum diff value mean found at each month, its needed
        to compare with the new diff values that we found each loop, and check if we have
        found a new maximum diference

    aux : int
        Auxiliar value that iterates with the loop to store its value on 'index' variable
        each time 'maxdiff' finds a new maximum diference value, it counts the number of loops

    dias : int
        Auxiliar value that counts the number of days of data in that month so we can calculate
        the mean

    diffparcial : int
        Auxiliar vlaue that stores the sumatory of the diference each month so we can calculate
        the mean

    years : int
        Auxiliar value where we calculate the year needed to build the datetime.date for the
        return value from the .index obtained at 'index' position

    month : int
        Auxiliar value where we calculate the month needed to build the datetime.date for the
        return value from the .index obtained at 'index' position

        """
        response = list()
        self._logger.info("Starting highest_monthly_mean_variation query")
        for data in self._data_frame:
            assert data is not None
            self._logger.info("Extracting data from dataframe")
            index = 0
            maxmean = 0.0
            aux = 0

            # recorremos anualmente los valores
            for i in range(data.head(1).index.year.values.astype(int)[0],
                           data.tail(1).index.year.values.astype(int)[0] + 1):
                # recorremos mes a mes del año
                for j in range(1, 13):

                    high = data['high']
                    low = data['low']

                    from_date = dt.date(year=i, month=j, day=1)
                    to_date = dt.date(year=i, month=j, day=monthrange(i, j)[1])

                    high = high.loc[from_date:to_date]  # type: ignore
                    low = low.loc[from_date:to_date]  # type: ignore

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

            high = data['high']
            low = data['low']
            years = high.index.values[index].astype('datetime64[Y]').astype(int) + 1970
            months = high.index.values[index].astype('datetime64[M]').astype(int) % 12 + 1

            date = dt.date(year=years, month=months, day=1)

            response.append([date, maxmean])

        return response
