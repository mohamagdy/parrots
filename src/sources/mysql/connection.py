from time import sleep
from typing import Union, Optional

import pymysql
import sentry_sdk
from loguru import logger
from pymysql import InternalError, OperationalError
from pymysql.cursors import DictCursor, SSCursor


class Connection:
    MAXIMUM_RECONNECTION_RETRIES = 10
    SLEEP_TIME_BETWEEN_RECONNECTION_RETRIES_IN_SECONDS = 5

    def __init__(self, host: str, port: int, username: str, password: str, character_set: str, connection_timeout: int):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.character_set = character_set
        self.connection_timeout = connection_timeout

        self.connection = None

    def connect(self, retries: int=0):
        """
        Connects to MySQL and sets the `connection` variable.

        :param retries: The retry counter. Default set to `0`
        """
        if not self.connection or not self.connection.open:
            logger.trace(f"Create MySQL connection. Retry count: {retries}")
            try:
                self.connection = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                    charset=self.character_set,
                    connect_timeout=self.connection_timeout
                )
                self.connection.connect()
            except OperationalError as error:
                if retries >= self.MAXIMUM_RECONNECTION_RETRIES:
                    message = f"Maximum connection retries to reached. " \
                              f"MySQL OperationalError during connection. Error: {error}."
                    logger.error(message)

                    sentry_sdk.capture_exception(OperationalError(message))
                else:
                    retries += 1

                    message = f"MySQL OperationalError during connection. Error: {error}. " \
                              f"Retrying in {self.SLEEP_TIME_BETWEEN_RECONNECTION_RETRIES_IN_SECONDS} seconds" \
                              f"Number of retries: {retries} / {self.MAXIMUM_RECONNECTION_RETRIES}"

                    logger.warning(message)
                    sentry_sdk.capture_exception(OperationalError(message))

                    sleep(self.SLEEP_TIME_BETWEEN_RECONNECTION_RETRIES_IN_SECONDS)
                    self.connect(retries=retries)

    def cursor(self) -> DictCursor:
        """
        Returns a connection buffered cursor.

        :return: MySQL connection buffered cursor.
        :rtype: DictCursor
        """
        return self.connection.cursor(cursor=DictCursor)

    def unbuffered_cursor(self) -> SSCursor:
        """
        Returns a connection unbuffered cursor.

        :return: MySQL connection unbuffered cursor.
        :rtype: SSCursor
        """
        return self.connection.cursor(cursor=SSCursor)

    def reconnect(self):
        """
        Reconnects to MySQL by recalling the `connect` method.
        """
        self.connect()

    def commit(self):
        """
        Commits transaction to MySQL
        """
        self.connection.commit()

    def execute_query(self, query: str, query_variables: object=None, fetch_type: str="",
                      fetch_many_size: Optional[int]=None) -> Union[bool, list, dict, None]:
        """
        Executes the given query string in MySQL.

        :param query: The query to execute
        :param query_variables: The query variables if any. Default is `None`
        :param fetch_type: The fetch type of the results (for `SELECT` statements). Default is empty string.
        :param fetch_many_size: The fetch many size. Default is `None`

        :return: Depending on `fetch_type` if empty string it returns a boolean with value `True` if the query is
            successfully executed, `False` otherwise. if `fetch_type` has value "all" returns a list otherwise
            a tuple or `None`
        :rtype: Union[bool, list, tuple, None]
        """
        cursor = self.cursor()

        try:
            cursor.execute(query, args=query_variables)

            if fetch_type:
                if fetch_type == "all":
                    return cursor.fetchall()
                else:
                    return cursor.fetchone()
            else:
                return True
        except OperationalError as error:
            logger.error(f"MySQL OperationalError error occurred during query: {query}! Error: {error}.")

            self.reconnect()

            return self.execute_query(
                query=query,
                query_variables=query_variables,
                fetch_type=fetch_type,
                fetch_many_size=fetch_many_size
            )
        except InternalError as error:
            logger.error(f"MySQL InternalError error occurred during query: {query}! Error: {error}.")

            self.connection.rollback()

            return self.execute_query(
                query=query,
                query_variables=query_variables,
                fetch_type=fetch_type,
                fetch_many_size=fetch_many_size
            )
        finally:
            cursor.close()

    def fetch_all_query(self, query: str) -> list:
        """
        Execute the query and fetch all results as a list.

        :param query: The query to execute

        :return: A list of tuples represents the query results
        :rtype: list
        """
        return self.execute_query(query=query, fetch_type="all")

    def fetch_one_query(self, query: str) -> Optional[dict]:
        """
        Execute the query and fetch one result as a tuple or `None`.

        :param query: The query to execute

        :return: A tuple or `None` represents the query result
        :rtype: Union[tuple, None]
        """
        return self.execute_query(query=query, fetch_type="one")

    def close(self):
        """
        Closes the MySQL connection.
        """
        self.connection.close()
