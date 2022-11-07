import asyncio
import concurrent
import csv
import gzip
import os
import re
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from io import StringIO
from operator import itemgetter
from time import sleep
from typing import Dict, Optional

from loguru import logger
from pymysqlreplication.event import RotateEvent, QueryEvent, BinLogEvent
from pymysqlreplication.row_event import RowsEvent, DeleteRowsEvent

from monitoring.mysql_to_singlestore_prometheus_agent import MySQLToSinglestorePrometheusAgent
from sinks.singlestore.positions_table import PositionsTable
from sinks.singlestore.table import Table as SinglestoreTable
from sources.mysql.binlog import Binlog
from sources.mysql.connection import Connection as MySQLConnection
from sources.mysql.converter.mysql_to_singlestore_converter import MySQLToSinglestoreConverter


class MySQLBinlogEventsHandler:
    DATA_FILE_EXTENSION_AND_FORMAT = "gzip"

    ALTER_COMMAND = "ALTER"
    DROP_COMMAND = "DROP"
    ALTER_TABLE_REGEX = r"ALTER TABLE (\w+) (ADD|DROP|MODIFY|CHANGE)\s(?:COLUMN\s)?(\w+)(?:$|\s(\S+))"

    LOG_FILE_NAME_FIELD_NAME = "log_file_name"
    LOG_POSITION_FIELD_NAME = "log_position"
    LOG_TIMESTAMP_FIELD_NAME = "log_timestamp"
    LOG_EVENT_CLASS = "log_event_class"

    EVENT_DATA_KEY_NAME = "data"

    def __init__(self, source_name: str, mysql_schemas_and_tables: dict, singlestore_tables: dict,
                 positions_table: PositionsTable, mysql_connection: MySQLConnection, auto_clean_files: bool,
                 writer_sleep_interval_in_seconds: int, prometheus_agent: MySQLToSinglestorePrometheusAgent):

        self.source_name = source_name
        self.mysql_schemas_and_tables = mysql_schemas_and_tables
        self.singlestore_tables = singlestore_tables
        self.replicator_positions_table = positions_table

        self.mysql_connection = mysql_connection

        self.auto_clean_files = auto_clean_files

        self.writer_sleep_interval_in_seconds = writer_sleep_interval_in_seconds

        self.writer_thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="WriterThred")

        self.prometheus_agent = prometheus_agent

        self.events_buffer = defaultdict(defaultdict)

    def initialize_buffer(self):
        """
        Initializes the buffer where the `RowsEvents` will be stored in until the writer thread wakes up and
        flushes the events to the database. The buffer is a dictionary, the key is the table name and the value
        is another dictionary the key is the primary key(s) values and the value is the full record values. If the
        primary key consists of more than 1 column, the dictionary key will be a tuple with the primary key columns
        values. For example, if the table's primary key has 1 column `a` the key will be the value of the column `a`.
        If the table's primary key is a set of columns `a` and `b`, then the dictionary key will be a tuple of the
        columns `a` and `b`.

        The reason for using the primary keys as dictionary keys is to make sure that when writing the buffer the latest
        record values are used. The value of the dictionary changes everytime an event for the same record (same
        primary key) is received.
        """
        for singlestore_table_name, singlestore_table in self.singlestore_tables.items():
            self.events_buffer[singlestore_table_name] = defaultdict()

        asyncio.run(self.__initialize_writer_thread())

    def set_values_to_monitoring_keys(self):
        """
        This method is called to set the Prometheus agent with the available coordinates for all the tables
        replicated from MySQL to SingleStore.
        """
        for singlestore_table_name, singlestore_table in self.singlestore_tables.items():
            singlestore_schema_name = singlestore_table.schema_name

            coordinates = self.replicator_positions_table.coordinates_for_table(
                schema_name=singlestore_schema_name,
                table_name=singlestore_table_name,
                log_source=self.source_name
            )

            if coordinates:
                self.prometheus_agent.set_last_source_transaction_timestamp(
                    schema_name=singlestore_schema_name,
                    table_name=singlestore_table_name,
                    value=coordinates["log_timestamp"]
                )

    def handle_event(self, event: BinLogEvent, current_binlog_file_name: str) -> list:
        """
        Handles the MySQL Binlog event consumed from the stream.

        :param event: The MySQL Binlog event
        :param current_binlog_file_name: The current MySQL binlog file name

        :return: The Binlog file name and the Binlog position inside the MySQL Binlog file
        :rtype: list
        """
        if isinstance(event, RotateEvent):
            current_binlog_file_name, current_binlog_position = self.__class__.__handle_rotate_event(event=event)
        elif isinstance(event, QueryEvent):
            current_binlog_file_name, current_binlog_position = self.__handle_alter_table_query(
                event=event,
                current_binlog_file_name=current_binlog_file_name
            )
        else:
            current_binlog_file_name, current_binlog_position = self.__handle_rows_event(
                event=event,
                current_binlog_file_name=current_binlog_file_name
            )

        return [current_binlog_file_name, current_binlog_position]

    @staticmethod
    def __handle_rotate_event(event: RotateEvent):
        """
        Handles the `RotateEvent`

        :param event: The MySQL `RotateEvent` event

        :return: The Binlog file name the Binlog position inside the MySQL Binlog file
        :rtype: list
        """

        current_binlog_file_name = event.next_binlog
        current_binlog_position = event.position

        logger.trace(f"RotateEvent received. Current coordinates: {current_binlog_file_name}:{current_binlog_position}")

        return [current_binlog_file_name, current_binlog_position]

    def __handle_alter_table_query(self, event: QueryEvent, current_binlog_file_name: str) -> list:
        """
        Handles the `ALTER TABLE` commands streamed from the MySQL Binlog. It executes the same
        `ALTER TABLE` statement against SingleStore with the proper SingleStore syntax and data type conversion.

        It ignores the columns that should be ignored for the table which is configured.

        :param event: The `QueryEvent` event
        :param current_binlog_file_name: The current Binlog file name

        :return: The Binlog file name and the Binlog position inside the MySQL Binlog file
        :rtype: list
        """
        if event.query.startswith(self.ALTER_COMMAND):
            pattern = re.compile(self.ALTER_TABLE_REGEX)
            match = pattern.match(event.query.replace("`", ""))

            if match:
                schema_name = event.schema.decode("UTF-8")
                table_name = match.group(1)
                action = match.group(2)
                column_name = match.group(3)

                if table_name in self.singlestore_tables.keys():
                    mysql_table = self.mysql_schemas_and_tables[schema_name][table_name]
                    singlestore_table = self.singlestore_tables[table_name]

                    # Before applying the `ALTER` command, make sure that the events buffer to the table is flushed
                    # so if a column is added, changed or dropped the data in the buffer won't be stale and cause
                    # errors when being loaded later after the `ALTER` command is executed.
                    self.__flush_table_buffer(table_name=table_name, events_buffer=self.events_buffer[table_name])

                    if column_name not in mysql_table.ignore_columns:
                        mysql_table.set_mysql_connection(self.mysql_connection)

                        singlestore_column_metadata = None

                        if action != self.DROP_COMMAND:
                            column_metadata = mysql_table.column_metadata(column_name=column_name)
                            if column_metadata:
                                singlestore_column_metadata = MySQLToSinglestoreConverter.convert_mysql_column_to_singlestore(
                                    column_metadata
                                )
                            else:
                                logger.debug("Cannot fetch column metadata. Seems the column is dropped from MySQL!")
                                return [current_binlog_file_name, event.packet.log_pos]

                        singlestore_table.alter(
                            action=action,
                            column_name=column_name,
                            column_type=singlestore_column_metadata
                        )
                    else:
                        logger.debug(f"Ignore ALTER command for table {singlestore_table.schema_name}.{table_name}")

        return [current_binlog_file_name, event.packet.log_pos]

    def __handle_rows_event(self, event: RowsEvent, current_binlog_file_name: str) -> list:
        """
        Handles the `INSERT`, `UPDATE` and `DELETE` commands streamed from the MySQL Binlog. It executes the same
        statements against SingleStore with the proper SingleStore syntax.

        :param event: The `RowsEvent` event
        :param current_binlog_file_name: The current Binlog file name

        :return: The Binlog file name and the Binlog position inside the MySQL Binlog file
        :rtype: list
        """
        singlestore_table = self.singlestore_tables[event.table]
        mysql_table = self.mysql_schemas_and_tables[event.schema][event.table]

        coordinates = self.replicator_positions_table.coordinates_for_table(
            schema_name=singlestore_table.schema_name,
            table_name=singlestore_table.table_name,
            log_source=self.source_name
        )

        log_file_name = coordinates[self.LOG_FILE_NAME_FIELD_NAME]

        table_binlog_sequence = Binlog.sequence_number(binlog_file_name=log_file_name)
        table_binlog_position = coordinates[self.LOG_POSITION_FIELD_NAME]

        current_binlog_sequence = Binlog.sequence_number(binlog_file_name=current_binlog_file_name)
        current_binlog_position = event.packet.log_pos

        process_transaction_condition = (
                                          current_binlog_sequence == table_binlog_sequence and
                                          current_binlog_position > table_binlog_position
                                        ) or (current_binlog_sequence > table_binlog_sequence)

        if process_transaction_condition:
            rows_values = Binlog.latest_snapshot_for_rows(rows=event.rows, ignore_columns=mysql_table.ignore_columns)

            for row_values in rows_values:
                primary_keys_values = itemgetter(*singlestore_table.primary_keys)(row_values)

                self.events_buffer[singlestore_table.table_name][primary_keys_values] = {
                    self.EVENT_DATA_KEY_NAME: row_values,
                    self.LOG_FILE_NAME_FIELD_NAME: current_binlog_file_name,
                    self.LOG_POSITION_FIELD_NAME: event.packet.log_pos,
                    self.LOG_TIMESTAMP_FIELD_NAME: event.timestamp,
                    self.LOG_EVENT_CLASS: event.__class__
                }
        else:
            logger.trace(
                f"Skip event for table {event.table}. "
                f"Current coordinates: {current_binlog_sequence}:{current_binlog_position} vs "
                f"Last processed coordinates: {table_binlog_sequence}:{table_binlog_position}"
            )

        return [current_binlog_file_name, current_binlog_position]

    async def __initialize_writer_thread(self):
        """
        Initializes the writer thread
        """
        asyncio.get_event_loop().run_in_executor(self.writer_thread, self.__run_writer_thread)

    def __run_writer_thread(self):
        """
        Starts the writer thread in an infinite loop.
        """
        while True:
            logger.debug(f"Writer thread sleeps for {self.writer_sleep_interval_in_seconds} seconds")
            sleep(self.writer_sleep_interval_in_seconds)
            asyncio.run(self.__flush_buffer())

    async def __flush_buffer(self):
        """
        Flushes the buffer into the final tables in SingleStore. This runs in the writer thread.
        """
        logger.debug("Writer thread woke up!")

        start_time = datetime.now()
        loop = asyncio.get_event_loop()

        worker_count = len(self.singlestore_tables)

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="FlushThread") as pool:
            table_writes_tasks = []
            # To avoid `RuntimeError: dictionary changed size during iteration` where the streaming thread
            # writes to the same dictionary while iterating over it, the items of the dictionary are converted
            # to a `list` so it iterates over a copy of the dictionary items
            for table_name, events_buffer in list(self.events_buffer.items()):
                task = loop.run_in_executor(pool, self.__flush_table_buffer, table_name, events_buffer)
                table_writes_tasks.append(task)

            if len(table_writes_tasks):
                await asyncio.wait(table_writes_tasks)

            seconds_elapsed = (datetime.now() - start_time).seconds
            logger.debug(f"Flush all tables events took {seconds_elapsed} seconds")

    def __flush_table_buffer(self, table_name: str, events_buffer: Optional[Dict]):
        """
        Flushes the table buffer into final table in SingleStore. This runs in a separate thread for each table.

        :param table_name: The final table in SingleStore where the buffer is going to be flushes
        :param events_buffer: The table buffer as a dictionary, the key is the primary key(s) values and the value
            is the full record value
        """
        singlestore_table = self.singlestore_tables[table_name]
        schema_name = singlestore_table.schema_name

        insert_or_updates_buffer = StringIO()
        inserts_or_updates_csv_writer = csv.writer(insert_or_updates_buffer)
        inserts_or_updates_csv_writer.writerow(singlestore_table.columns)

        deletes_buffer = StringIO()
        deletes_csv_writer = csv.writer(deletes_buffer)
        deletes_csv_writer.writerow(singlestore_table.columns)

        event_log_file_name = None
        event_log_position = None
        event_log_timestamp = None

        inserts_or_updates_count = 0
        deletes_count = 0

        with self.prometheus_agent.time_it(
            self.prometheus_agent.set_total_table_flush_time_seconds, schema_name=schema_name, table_name=table_name,
            message=f"Flush table {singlestore_table.full_name} events"
        ):
            try:
                if events_buffer:
                    # To avoid `RuntimeError: dictionary changed size during iteration` where the streaming thread
                    # writes to the same dictionary while iterating over it, the items of the dictionary are converted
                    # to a `list` so it iterates over a copy of the dictionary items
                    for primary_key_values_tuple, event in list(events_buffer.items()):
                        event_log_file_name = event[self.LOG_FILE_NAME_FIELD_NAME]
                        event_log_position = event[self.LOG_POSITION_FIELD_NAME]
                        event_log_timestamp = event[self.LOG_TIMESTAMP_FIELD_NAME]

                        if event[self.LOG_EVENT_CLASS] == DeleteRowsEvent:
                            deletes_count += 1
                            deletes_csv_writer.writerow(event[self.EVENT_DATA_KEY_NAME].values())
                        else:
                            inserts_or_updates_count += 1
                            inserts_or_updates_csv_writer.writerow(event[self.EVENT_DATA_KEY_NAME].values())

                        self.events_buffer[table_name].pop(primary_key_values_tuple)
            except Exception as exception:
                logger.error(exception)
            finally:
                with self.prometheus_agent.time_it(
                    self.prometheus_agent.set_update_or_insert_seconds,
                    schema_name=schema_name,
                    table_name=table_name
                ):
                    if inserts_or_updates_count:
                        self.__flush_buffer_to_table(buffer=insert_or_updates_buffer, singlestore_table=singlestore_table)
                        update_or_insert_done = singlestore_table.update_or_insert_using_staging_table()
                    else:
                        update_or_insert_done = True

                with self.prometheus_agent.time_it(
                    self.prometheus_agent.set_delete_seconds, schema_name=schema_name, table_name=table_name
                ):
                    if deletes_count:
                        self.__flush_buffer_to_table(buffer=deletes_buffer, singlestore_table=singlestore_table)
                        delete_done = singlestore_table.delete_using_staging_table()
                    else:
                        delete_done = True

                with self.prometheus_agent.time_it(self.prometheus_agent.set_metadata_update_seconds):
                    if update_or_insert_done and delete_done and (inserts_or_updates_count or deletes_count):
                        self.replicator_positions_table.update_table_position(
                            schema_name=schema_name,
                            table_name=table_name,
                            log_file_name=event_log_file_name,
                            log_position=event_log_position,
                            log_timestamp=event_log_timestamp,
                            log_source=self.source_name
                        )

                        # Monitoring
                        self.prometheus_agent.set_last_source_transaction_timestamp(
                            schema_name=schema_name,
                            table_name=table_name,
                            value=event_log_timestamp
                        )
                        logger.debug(
                            f"Insert or updated {inserts_or_updates_count} records and deleted {deletes_count} records "
                            f"in {singlestore_table.full_name}"
                        )
                    else:
                        logger.trace(f"Nothing inserted, updated or deleted in/from table {table_name}!")

                insert_or_updates_buffer.close()
                deletes_buffer.close()

    def __flush_buffer_to_table(self, buffer: StringIO, singlestore_table: SinglestoreTable):
        """
        Flushes the given buffer `StringIO` object to a SingleStore table. The flushing includes writing to a local
        file in `/tmp` directory, then uploading the local file to S3 then finally copying the file over to the SingleStore
        table. The method auto cleans the files after each successful step.

        :param buffer: A `StringIO` object that contains the data to copy over to the SingleStore table
        :param singlestore_table: The SingleStore table to flush the buffer to.
        """
        file_name = f"/tmp/{singlestore_table.full_name}.{self.DATA_FILE_EXTENSION_AND_FORMAT}"

        with gzip.open(file_name, "wb") as output_file:
            output_file.write(buffer.getvalue().encode())

        singlestore_table.load_file(
            path=file_name,
            to_staging_table=True
        )

        if self.auto_clean_files:
            logger.debug(f"Deleting local file {file_name}")
            os.remove(file_name)
