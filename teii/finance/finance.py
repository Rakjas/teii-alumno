""" Finance Client classes """


import json
import logging
import os
import requests

from abc import ABC, abstractclassmethod
from pathlib import Path
from typing import Optional, Union

from teii.finance import FinanceClientInvalidAPIKey
from teii.finance import FinanceClientAPIError
from teii.finance import FinanceClientInvalidData
from teii.finance import FinanceClientIOError


class FinanceClient(ABC):
    """ Wrapper around the Finance API. """

    _FinanceBaseQueryURL = "https://www.alphavantage.co/query?"  # Class variable

    def __init__(self, ticker: list,
                 api_key: Optional[str] = None,
                 logging_level: Union[int, str] = logging.INFO,
                 logging_file: Optional[str] = None) -> None:
        """ FinanceClient constructor. """

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

    def _build_base_query_url(self) -> list:
        """Return base query URL.

        URL is independent from the query type.
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?PARAMS
        """

        return self._FinanceBaseQueryURL

    @abstractclassmethod
    def _build_base_query_url_params(self) -> str:
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
        """ Preprocess query data. """
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
        """ Return pandas data frame from json data. """
        response = list()
        for data in self._data_frame:
            assert data is not None
            response.append(data)
        return response

    def to_csv(self, path2file: Path) -> Path:
        """ Write json data into csv file 'path2file'. """
        for data in self._data_frame:
            assert data is not None

            try:
                data.to_csv(path2file, mode='a')
            except (IOError, PermissionError) as e:
                raise FinanceClientIOError(f"Unable to write json data into file '{path2file}'") from e

        return path2file
