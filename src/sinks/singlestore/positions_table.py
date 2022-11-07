import datetime
import functools

from loguru import logger

from sinks.singlestore.table import Table


class PositionsTable(Table):
    @property
    def columns_with_data_types(self) -> dict:
        """
        Returns the column names with data types for the table as a dictionary. The key is the column name and the
        value if the data type in Redshift syntax together with the character length of the column

        :return: A dictionary contains the table column names and data types. The key is the column name and the
        value if the data type in Redshift syntax together with the character length of the column
        :rtype: dict
        """
        return {
            "schema_name": "VARCHAR(255)",
            "table_name": "VARCHAR(255)",
            "log_file_name": "VARCHAR(255)",
            "log_position": "INTEGER",
            "log_timestamp": "BIGINT",
            "log_source": "VARCHAR(255)"
        }

    @property
    def primary_keys(self) -> list:
        """
        Returns the primary keys of the table
        :return: The primary keys of the table
        :rtype: list
        """
        return ["schema_name", "table_name"]

    def create_if_not_exists(self, **kwargs) -> bool:
        """
        Creates the table in the Redshift database if the table does not exist.

        :param **kwargs: Not used. Added to match the super class signature.

        :return: A boolean with value `True` if the query is successfully executed, `False` otherwise
        :rtype: bool
        """
        return super(PositionsTable, self).create_if_not_exists(
            columns_names_and_types=self.columns_with_data_types,
            primary_keys=self.primary_keys,
            create_staging_table=False
        )

    def insert_or_update(self, columns_names_and_values: dict) -> bool:
        """
        Inserts a new record to the table. The row to be inserted ins passed as a dictionary. The key is the column name
        and the value is the column value.

        :param columns_names_and_values: A dictionary, the key is the column name and the value is the value of the
            column

        :return: A boolean with value `True` if the query is successfully executed, `False` otherwise
        :rtype: bool
        """
        schema_name = columns_names_and_values["schema_name"]
        table_name = columns_names_and_values["table_name"]

        select_values_statement = f"""
            SELECT COUNT(*) AS cnt FROM {self.schema_name}.{self.table_name}
            WHERE schema_name = '{schema_name}' AND table_name = '{table_name}'
        """

        self.cursor.execute(select_values_statement)
        result = self.cursor.fetchone()
        self.connection.commit()

        if result and result["cnt"] > 0:
            log_file_name = columns_names_and_values["log_file_name"]
            log_position = columns_names_and_values["log_position"]
            log_timestamp = columns_names_and_values["log_timestamp"]
            log_source = columns_names_and_values["log_source"]

            return self.update_table_position(
                schema_name=schema_name, table_name=table_name, log_file_name=log_file_name, log_position=log_position,
                log_timestamp=log_timestamp, log_source=log_source
            )
        else:
            return super().insert(columns_names_and_values)

    def insert_table_and_schema_if_does_not_exist(self, schema_name: str, table_name: str) -> bool:
        """
        Inserts the given `schema_name` and `table_name` to the `positions` table if does not exist.

        :param schema_name:
        :param table_name:

        :return: A boolean with value `True` if the query is successfully executed, `False` otherwise
        :rtype: bool
        """
        select_values_statement = f"""
            SELECT COUNT(*) AS cnt FROM {self.schema_name}.{self.table_name}
            WHERE schema_name = '{schema_name}' AND table_name = '{table_name}'
        """

        self.cursor.execute(select_values_statement)
        result = self.cursor.fetchone()
        self.connection.commit()

        if not result[0]:
            return super().insert({"schema_name": schema_name, "table_name": table_name})
        else:
            return True

    @functools.lru_cache()
    def coordinates_for_table(self, schema_name: str, table_name: str, log_source: str) -> dict:
        """
        Returns the log file name and position for a given table by its schema and table name

        :param schema_name: Schema name of the table to retrieve the coordinates for
        :param table_name: Table name of the table to retrieve the coordinates for
        :param log_source: The source from which the log is set

        :return: The last value of the log coordinates for a given table. The key is the name of the
        coordinate and the value is the value. `log_file_name` as a key is the file name,
        `log_position` is the position and `log_timestamp` is the timestamp of the transaction

        :rtype: dict
        """
        select_statement = f"""
            SELECT log_file_name, log_position, log_timestamp
            FROM {self.schema_name}.{self.table_name}
            WHERE schema_name = '{schema_name}' AND table_name = '{table_name}' AND log_source = '{log_source}'
        """

        logger.trace(f"Fetch {schema_name}.{table_name} log coordinates from {self.full_name} in Redshift")

        self.cursor.execute(select_statement)
        result = self.cursor.fetchone()
        self.connection.commit()

        return dict(result or {})

    def update_table_position(self, schema_name: str, table_name: str, log_file_name: str, log_position: int,
                              log_timestamp: int, log_source: str) -> bool:
        """
        Updates the log coordinates (file name and position) for a given table (schema name and table name).

        :param schema_name: The schema of the table to update the coordinates for
        :param table_name: The table of the table to update the coordinates for
        :param log_position: The new value of the MySQL Binlog position
        :param log_timestamp: The log timestamp since epoch
        :param log_source: The source from which the log is set
        :param log_file_name: The new value of the MySQL Binlog file name. Default is empty string

        :return: A boolean with value `True` if the query is successfully executed, `False` otherwise
        :rtype: bool
        """
        logger.debug(
            f"Update coordinates for {schema_name}.{table_name} to {log_file_name}:{log_position}"
            f" event time: {datetime.datetime.utcfromtimestamp(log_timestamp)} and log source: {log_source}"
        )

        self.cursor.execute(f"""
            UPDATE {self.schema_name}.{self.table_name}
            SET 
                log_file_name = '{log_file_name}',
                log_position = '{log_position}',
                log_timestamp = '{log_timestamp}',
                log_source = '{log_source}'
            WHERE schema_name = '{schema_name}' AND table_name = '{table_name}';
        """)
        self.connection.commit()

        return True

    def update_table_timestamp(self, schema_name: str, table_name: str, log_timestamp: int, log_source: str) -> bool:
        """
        Updates the log timestamp for a given table (schema name and table name).

        :param schema_name: The schema of the table to update the coordinates for
        :param table_name: The table of the table to update the coordinates for
        :param log_timestamp: The log timestamp since epoch
        :param log_source: The source from which the log is set

        :return: A boolean with value `True` if the query is successfully executed, `False` otherwise
        :rtype: bool
        """
        event_date_time = datetime.datetime.utcfromtimestamp(log_timestamp)

        logger.debug(
            f"Update timestamp for {schema_name}.{table_name} to event time: {event_date_time} and source: {log_source}"
        )

        self.cursor.execute(f"""
            UPDATE {self.schema_name}.{self.table_name}
            SET
                log_timestamp = '{log_timestamp}',
                log_source = '{log_source}'
            WHERE schema_name = '{schema_name}' AND table_name = '{table_name}';
        """)
        self.connection.commit()

        return True

    def oldest_log_file_name(self, log_source: str) -> str:
        """
        Returns the oldest log file name stored in the table that stores the coordinates not earlier than the day
        before to avoid reading too much log history which was already processed in case the MySQL Binlog retention
        is set to more than 1 day.

        :param log_source: The log source name

        :return: The name of the oldest log file name
        :rtype: str
        """
        oldest_binlog_file_sql = f"""
            SELECT log_file_name
            FROM {self.schema_name}.{self.table_name}
            WHERE log_source = '{log_source}' AND log_timestamp > unix_timestamp(CURRENT_DATE - 1)
            ORDER BY log_timestamp
            LIMIT 1
        """

        logger.debug(f"Fetch oldest log file name from {self.schema_name}.{self.table_name}")

        self.cursor.execute(oldest_binlog_file_sql)
        result = self.cursor.fetchone() or {}
        self.connection.commit()

        log_file_name = result.get("log_file_name", "")

        logger.debug(f"Oldest log file name is {log_file_name}")

        return log_file_name
