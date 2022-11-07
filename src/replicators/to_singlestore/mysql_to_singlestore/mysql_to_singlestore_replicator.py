import asyncio
import datetime
import functools
import os
import time
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor

import sentry_sdk
from loguru import logger
from pymysql import InternalError, InterfaceError, OperationalError

from monitoring.mysql_to_singlestore_prometheus_agent import MySQLToSinglestorePrometheusAgent
from replicators.to_singlestore.base_to_singlestore_replicator import BaseToSinglestoreReplicator
from replicators.to_singlestore.mysql_to_singlestore.mysql_binlog_events_handler import MySQLBinlogEventsHandler
from sinks.singlestore.table import Table as SinglestoreTable
from sources.mysql.binlog import Binlog
from sources.mysql.connection import Connection as MySQLConnection
from sources.mysql.converter.mysql_to_singlestore_converter import MySQLToSinglestoreConverter
from sources.mysql.table import Table as MySQLTable


class MySQLToSinglestoreReplicator(BaseToSinglestoreReplicator):
    def __init__(self, mysql_configurations: dict, singlestore_configurations: dict,
                 replication_information_configurations: dict, server_id: int, source_name: str, auto_clean_files: bool,
                 writer_sleep_interval_in_seconds: int, prometheus_agent: MySQLToSinglestorePrometheusAgent,
                 keep_existing_data: bool):

        super().__init__(
            singlestore_configurations=singlestore_configurations,
            replication_information_configurations=replication_information_configurations,
            auto_clean_files=auto_clean_files,
            writer_sleep_interval_in_seconds=writer_sleep_interval_in_seconds,
            prometheus_agent=prometheus_agent,
            keep_existing_data=keep_existing_data
        )

        self.mysql_configurations = mysql_configurations
        self.server_id = server_id
        self.source_name = source_name

        self.prometheus_agent = prometheus_agent

        self.mysql_schemas_and_tables = defaultdict(defaultdict)

    @property
    def mysql_sources(self) -> dict:
        """
        Returns the MySQL sources to replicate as a dictionary, the key is the schema name in MySQL and the value
        is a list of dictionaries of the tables to replicate.

        :return: A dictionary of tables to replicate
        :rtype: dict
        """
        return self.mysql_configurations["sources"]

    @property
    def number_of_tables_replicated_in_parallel(self) -> int:
        """
        Returns the number of tables to replicate in parallel at the same time set in the MySQL configurations.

        :return: The number of tables to replicate in parallel at the same time.
        :rtype: int
        """
        return int(self.mysql_configurations["number_of_tables_replicated_in_parallel"])

    def mysql_connection(self) -> MySQLConnection:
        """
        Creates a MySQL database connection

        :return: MySQL connection object
        :rtype: MySQLConnection
        """
        connection = MySQLConnection(
            host=self.mysql_configurations["host"],
            port=self.mysql_configurations["port"],
            username=self.mysql_configurations["username"],
            password=self.mysql_configurations["password"],
            character_set=self.mysql_configurations["character_set"],
            connection_timeout=self.mysql_configurations["connection_timeout"]
        )

        connection.connect()

        return connection

    def initialize_mysql_tables(self):
        """
        Creates the `MySQLTable` objects and populate the MySQL tables cache (dictionary)
        """
        for schema_name, tables in self.mysql_sources.items():
            for source_table in tables:
                table_name = next(iter(source_table.keys()))
                initial_replication_where_condition = source_table[table_name]["initial_replication_where_condition"]
                ignore_columns = self.__ignored_columns_as_tuple(source_table[table_name]["ignore_columns"])

                mysql_table = MySQLTable(
                    schema_name=schema_name,
                    table_name=table_name,
                    initial_replication_where_condition=initial_replication_where_condition,
                    ignore_columns=ignore_columns
                )

                self.mysql_schemas_and_tables[schema_name].update({table_name: mysql_table})

    @functools.lru_cache()
    def mysql_schemas_names(self) -> list:
        """
        Returns a list of MySQL schemas to replicate or stream

        :return: List of schema
        :rtype: list
        """
        return list(self.mysql_schemas_and_tables.keys())

    @functools.lru_cache()
    def mysql_tables_names(self) -> list:
        """
        Returns a list of all MySQL tables names to replicate or stream

        :return: A list of all MySQL tables names to replicate or stream
        :rtype: list
        """
        tables = []

        [tables.append(list(self.mysql_schemas_and_tables[schema].keys())) for schema in self.mysql_schemas_names()]

        return functools.reduce(lambda tables_left, tables_right: tables_left + tables_right, tables)

    def initialize_singlestore_tables(self):
        """
        Creates the `SinglestoreTable` objects and populate the Singlestore tables cache (dictionary)
        """
        for schema_name, tables in self.mysql_sources.items():
            for source_table in tables:
                table_name = next(iter(source_table.keys()))
                self.singlestore_tables[table_name] = SinglestoreTable(
                    connection=self.singlestore_connection,
                    schema_name=self.singlestore_schema_name,
                    table_name=table_name
                )

    def stream(self):
        """
        Starts a MySQL Binary Log stream and process the stream events in an infinite loop.

        There are 5 types of Binlog events processed:
            1. `RotateEvent`
            2. `WriteRowsEvent`
            3. `UpdateRowsEvent`
            4. `DeleteRowsEvent`
            5. `QueryEvent`
        """
        self.prometheus_agent.set_streaming_started()

        mysql_connection = self.mysql_connection()

        binlog = Binlog(mysql_connection=mysql_connection)

        # Start from the newest file either the oldest binlog file stored in the coordinates table or the first
        # available binary log file. If the first binary log file used is the one stored in the coordinates this doesn't
        # mean the log file is still available in MySQL
        initial_binlog_file_name = max(
            self.replicator_positions_table.oldest_log_file_name(log_source=self.source_name),
            binlog.first_available_binlog_file_name()
        )

        current_binlog_file_name = initial_binlog_file_name
        current_binlog_position = 4

        rows_query_event = MySQLBinlogEventsHandler(
            source_name=self.source_name,
            mysql_schemas_and_tables=self.mysql_schemas_and_tables,
            singlestore_tables=self.singlestore_tables,
            positions_table=self.replicator_positions_table,
            mysql_connection=mysql_connection,
            auto_clean_files=self.auto_clean_files,
            writer_sleep_interval_in_seconds=self.writer_sleep_interval_in_seconds,
            prometheus_agent=self.prometheus_agent
        )

        rows_query_event.initialize_buffer()
        rows_query_event.set_values_to_monitoring_keys()

        try:
            while True:
                stream = None
                try:
                    stream = binlog.stream(
                        binlog_file_name=current_binlog_file_name,
                        binlog_position=current_binlog_position,
                        schemas_names=self.mysql_schemas_names(),
                        tables_names=self.mysql_tables_names(),
                        server_id=self.server_id
                    )

                    for event in stream:
                        current_binlog_file_name, current_binlog_position = rows_query_event.handle_event(
                            event, current_binlog_file_name
                        )

                except InternalError as error:
                    internal_error_message = f"MySQL Binlog internal error occurred {error}"
                    logger.info(internal_error_message)
                    sentry_sdk.capture_exception(InternalError(internal_error_message))

                    self.prometheus_agent.set_last_caught_exception_timestamp()

                    if stream:
                        stream.close()
                except OperationalError as error:
                    operational_error_message = f"MySQL Binlog operational error occurred {error}"
                    logger.info(operational_error_message)
                    sentry_sdk.capture_exception(OperationalError(operational_error_message))

                    self.prometheus_agent.set_last_caught_exception_timestamp()

                    if stream:
                        stream.close()
                except KeyboardInterrupt:
                    terminate_signal_message = "Received terminate stream signal"
                    logger.info(terminate_signal_message)

                    if stream:
                        stream.close()

                    break
        finally:
            mysql_connection.close()

    def replicate_tables(self, source_schema_and_tables: dict):
        """
        Replicates tables specified in a dictionary format. The key is the schema name and the value is the list of
        tables resides in the schema. For example `{"schema1": ["table1", "table2", ... ], "schema2": ["table1", ...]}`.

        :param source_schema_and_tables: Dictionary of schema and tables. The tables is a list of dictionaries.
            The key for each dictionary is the table name and the value is a dictionary with the following keys:
            - `initial_replication_where_condition`: The `WHERE` condition to execute for the initial load.
            - `ignore_columns`: The list of columns to ignore when replicating the data.
        """
        start_time = datetime.datetime.now()

        logger.debug("Tables replication started!")

        self.prometheus_agent.set_initial_replication_started()

        asyncio.run(self.__replicate_tables_asynchronously(source_schema_and_tables))

        seconds_elapsed = (datetime.datetime.now() - start_time).seconds
        logger.info(f"Tables replication finished in {seconds_elapsed} seconds")

    async def __replicate_tables_asynchronously(self, source_schema_and_tables: dict):
        """
        Creates a thread for each table to be replicated.

        :param source_schema_and_tables: Dictionary of schema and tables. The tables is a list of dictionaries.
            The key for each dictionary is the table name and the value is a dictionary with the following keys:
            - `initial_replication_where_condition`: The `WHERE` condition to execute for the initial load.
            - `ignore_columns`: The list of columns to ignore when replicating the data.
        """
        loop = asyncio.get_event_loop()
        replication_tasks = []

        for schema_name, mysql_tables in source_schema_and_tables.items():
            with ThreadPoolExecutor(max_workers=self.number_of_tables_replicated_in_parallel) as pool:
                for mysql_table_name, mysql_table in mysql_tables.items():
                    replication_tasks.append(
                        loop.run_in_executor(pool, self.__replicate_table, schema_name, mysql_table)
                    )

        await asyncio.wait(replication_tasks)

    def __replicate_table(self, mysql_schema_name: str, mysql_table: MySQLTable):
        """
        Replicate the tables in the given `source_schema_name` and the given list in  `source_table_names` to the
        target schema defined by `target_schema_name` parameter

        :param mysql_schema_name: The MySQL source schema name
        :param mysql_table: A `MySQLTable` object that represents the MySQL table to replicate
        """
        start_time = datetime.datetime.now()

        mysql_connection = self.mysql_connection()

        table_name = mysql_table.table_name

        try:
            logger.debug(f"""
                Replicate {mysql_schema_name}.{table_name} to {self.singlestore_schema_name}.{table_name}. 
                Initial replication condition: {mysql_table.initial_replication_where_condition}
            """)

            mysql_table.set_mysql_connection(mysql_connection=mysql_connection)

            columns_metadata = mysql_table.columns_metadata()

            columns_names_and_types, primary_keys = MySQLToSinglestoreConverter.convert_mysql_table_to_singlestore(
                mysql_columns_metadata=columns_metadata
            )

            singlestore_table = self.singlestore_tables[table_name]

            singlestore_table.create_if_not_exists(
                columns_names_and_types=columns_names_and_types,
                primary_keys=primary_keys
            )

            # Get the Binlog file name and position
            binlog_status = Binlog(mysql_connection=mysql_connection).show_master_status()

            with self.prometheus_agent.time_it(self.prometheus_agent.set_metadata_update_seconds):
                self.replicator_positions_table.insert_or_update(columns_names_and_values={
                    "schema_name": singlestore_table.schema_name,
                    "table_name": singlestore_table.table_name,
                    "log_file_name": binlog_status["File"],
                    "log_position": binlog_status["Position"],
                    "log_timestamp": int(time.time()),
                    "log_source": self.source_name
                })

            # Export MySQL table to CSV
            table_local_export_path = mysql_table.export_to_csv()

            singlestore_table.load_file(
                path=table_local_export_path,
                to_staging_table=self.keep_existing_data
            )

            if self.keep_existing_data:
                singlestore_table.replicate_using_staging_table(
                    delete_condition=mysql_table.initial_replication_where_condition
                )

            if self.auto_clean_files:
                logger.debug(f"Deleting local file {table_local_export_path}")
                os.remove(table_local_export_path)

            return True
        except InterfaceError as error:
            logger.error(f"MySQL InterfaceError: {error}")
            sentry_sdk.capture_exception(error)

            return False
        finally:
            seconds_elapsed = (datetime.datetime.now() - start_time).seconds
            logger.info(
                f"Table {self.singlestore_schema_name}.{table_name} replication finished in {seconds_elapsed} seconds"
            )

            mysql_connection.connection.commit()
            mysql_connection.close()

    def replicate_tables_dry_run(self, source_schema_and_tables: dict) -> bool:
        """
        Dry run table replication from MySQL specified in a dictionary format. The key is the schema name and the value
        is the list of tables resides in the schema.
        For example `{"schema1": ["table1", "table2", ... ], "schema2": ["table1", ...]}`

        :param source_schema_and_tables: Dictionary of schema and tables. The tables is a list `MySQLTable` objects

        :return: A boolean after finishing the dry run
        :rtype: bool
        """
        logger.info(f"Dry run tables replication")

        mysql_connection = self.mysql_connection()

        try:
            for schema_name, mysql_tables in source_schema_and_tables:
                for mysql_table in mysql_tables:
                    table_name = mysql_table.table_name

                    logger.debug(f"Dry-run table {schema_name}.{table_name} replication")

                    mysql_table.set_mysql_connection(mysql_connection)

                    mysql_table.export_to_csv(dry_run=True)

                    logger.debug(f"Finished dry-run for table {schema_name}.{table_name}")

            return True
        except InterfaceError as error:
            logger.error(f"MySQL InterfaceError: {error}")
            sentry_sdk.capture_exception(error)

            return False
        finally:
            mysql_connection.close()

    @staticmethod
    def __ignored_columns_as_tuple(ignored_columns: list) -> tuple:
        """
        Converts the ignored columns list to a tuple. If the list contains only one record, it returns a tuple
        of 2 items of the same value. For example if the `ignored_columns = ["col"]` it returns `("col", "col")`
        other wise it returns 1 tuple item per column  `ignored_columns = ["col1", "col2"]` returns `("col1", "col2")`

        :param ignored_columns: The list of columns to ignore

        :return: A tuple of columns to ignore
        :type: tuple
        """
        ignored_columns = ignored_columns or []
        return tuple(ignored_columns * 2 if len(ignored_columns) == 1 else ignored_columns)
