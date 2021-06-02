""" Finance Client classes """


import json
import logging
import os
import requests
import pandas  # type: ignore

from abc import ABC, abstractclassmethod
from pathlib import Path
from typing import Optional, Union

from typing import List
from teii.finance import FinanceClientInvalidAPIKey
from teii.finance import FinanceClientAPIError
from teii.finance import FinanceClientInvalidData
from teii.finance import FinanceClientIOError


class FinanceClient(ABC):
    """
    Wrapper around the Finance API.

    Parameters
    ----------
    ticker : list of str
        List of tickers wich will be used to query data from alpha vantage, each of
        them will be answered with a json with financial data of the client represented
        by that ticker (for example AMZN -> Amazon)

    api_key : [optional] str, default = None
        Api_key that will be used to request the data in the API

    logging_level : [optional] Union of int and str, default logging.WARNING
        Level of info that will be written by the logger in a log fil

    logging_file : [optional] str, default = None
        Path to the file were the log file will be written

    Attributes
    ----------
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

    __init__(ticker, api_key, logging_level, logging_file)
        Constructor for TimeSeriesFinanceClient.

    to_pandas()
        Returns a list of data from ._data_frame.

    to_csv(path2file)
        Writes the data from ._data_frame into the file specified in
        'path2file' in format csv, appends each of the data to the file
        one after the other.

    """

    _FinanceBaseQueryURL = "https://www.alphavantage.co/query?"  # Class variable

    def __init__(self, ticker: list,
                 api_key: Optional[str] = None,
                 logging_level: Union[int, str] = logging.INFO,
                 logging_file: Optional[str] = None) -> None:
        """
        FinanceClient constructor.

        Parameters
        ----------

    ticker : list of str
        List of tickers wich will be used to query data from alpha vantage, each of
        them will be answered with a json with financial data of the client represented
        by that ticker (for example AMZN -> Amazon).

    api_key : [optional] str, default = None
        Api_key that will be used to request the data in the API.

    logging_level : [optional] Union of int and str, default logging.WARNING
        Level of info that will be written by the logger in a log file.

    logging_file : [optional], default = None
        Path to the file were the log file will be written

        Notes
        -----
        This class is the father of 'TimeSeriesFinanceClient.
        This constructor is called with super() by his child and communicates
        with Alpha Vantage data and realices the query obtaining the json data needed.
        Also defines the abstract methods that will be called in the construction on his child.
        """

        self._ticker = ticker
        self._api_key = api_key

        # Logging configuration
        self._setup_logging(logging_level, logging_file)

        # Finance API key configuration
        self._logger.info("API key configuration")
        if not self._api_key:
            self._api_key = os.getenv("TEII_FINANCE_API_KEY")
        if not self._api_key or not isinstance(self._api_key, str):
            raise FinanceClientInvalidAPIKey(f"{self.__class__.__qualname__} operation failed")

        # Query Finance API
        self._logger.info("Finance API access...")
        response = self._query_api()

        # Process query response
        self._logger.info("Finance API query response processing...")
        self._process_query_response(response)

        # Validate query data
        self._logger.info("Finance API query data validation...")
        self._validate_query_data()

        # Panda's Data Frame
        self._data_frame = None

    def _setup_logging(self,
                       logging_level: Union[int, str],
                       logging_file: Optional[str]) -> None:
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging_level)
        # handler = logging.FileHandler('teii.log')
        # formatter = logging.Formatter('%(asctime)s : teii-logging : %(levelname)s : %(message)s')
        # handler.setFormatter(formatter)
        # self._logger.addHandler(handler)

    def _build_base_query_url(self) -> str:
        """Return base query URL.

        URL is independent from the query type.
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?PARAMS
        """

        return self._FinanceBaseQueryURL

    @abstractclassmethod
    def _build_base_query_url_params(self) -> list:
        """ Return base query URL parameters.
        Parameters are dependent on the query type:
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?PARAMS
        """

        pass

    def _query_api(self) -> list:
        """ Query API endpoint. """
        response = list()
        i = 0
        to_iterate = self._build_base_query_url_params()
        for query in to_iterate:
            try:
                response.append(requests.get(f"{self._build_base_query_url()}{to_iterate[i]}"))
                self._logger.info(f"Appended response {self._build_base_query_url()}{to_iterate[i]}")
                assert response[i].status_code == 200

            except Exception as e:
                raise FinanceClientAPIError(f"Unsuccessful API access "
                                            f"[URL: {response[i].url}, status: {response[i].status_code}]") from e
            else:
                self._logger.info(f"Successful API access "
                                  f"[URL: {response[i].url}, status: {response[i].status_code}]")
            i = i + 1
        return response

    @classmethod
    def _build_query_metadata_key(self) -> str:
        """ Return metadata query key. """

        return "Meta Data"

    @abstractclassmethod
    def _build_query_data_key(self) -> str:
        """ Return data query key. """

        pass

    def _process_query_response(self, response: list) -> None:
        """
        Preprocess query data.

        Parameters
        ----------
    response : list of json data
        List containing the data obtained from de Alpha Vantage API

        Other Parameters
        ----------------
    json_data_downloaded : list of json
        List where we append the data obtained from the api on each loop
        iteration of responses

    _json_metadata : list of json metadata
        Class parameter that stores the list of metadata of each json

    _json_data : list of json data
        Class parameter that stores the list of data of each json

        """
        json_data_downloaded = list()
        self._json_metadata = list()
        self._json_data = list()
        i = 0
        for responses in response:
            try:
                self._logger.info("Trying to process API")
                json_data_downloaded.append(responses.json())
                self._json_metadata.append(json_data_downloaded[i][self._build_query_metadata_key()])
                self._json_data.append(json_data_downloaded[i][self._build_query_data_key()])
            except Exception as e:
                raise FinanceClientInvalidData("Invalid data") from e
            else:
                self._logger.info("Metadata and data fields found")

            self._logger.info(f"Metadata: {self._json_metadata[i]}")
            self._logger.info(f"Data: {json.dumps(self._json_data[i])[0:218]}...")
            i = i + 1

    @abstractclassmethod
    def _validate_query_data(self) -> None:
        """ Validate query data. """

        pass

    def to_pandas(self) -> list:
        """
        Return pandas data frame from json data.

        Returns
        -------
        List of pandas.dataframes with the data in '_data_frame'

        Other Parameters
        ----------------
    data : pandas.DataFrame
        Stores the data of each DataFrame in '_data_frame' on each
        loop iteration

    response : list of pandas.DataFrame
        Variable where we append the data in each iteration to be
        returned at the end.

        """
        response = list()  # type: List[pandas.DataFrame]
        assert self._data_frame is not None
        for data in self._data_frame:
            response.append(data)
        return response

    def to_csv(self, path2file: Path) -> Path:
        """
        Write json data into csv file 'path2file'.

        Parameters
        ----------
    path2file : str
        Path and name of the file where will be stored all the info
        from the '_data_frame' in csv format

        Returns
        -------
        Path where it has been stored the .csv file

        Other Parameters
        ----------------
    data : pandas.DataFrame
        Stores the data of each DataFrame in '_data_frame' on each
        loop iteration

        """
        assert self._data_frame is not None
        for data in self._data_frame:

            try:
                data.to_csv(path2file, mode='a')
            except (IOError, PermissionError) as e:
                raise FinanceClientIOError(f"Unable to write json data into file '{path2file}'") from e

        return path2file
