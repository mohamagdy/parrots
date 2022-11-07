import re
from typing import Optional

from loguru import logger
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent, QueryEvent
from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, RowsEvent

from sources.mysql.connection import Connection as MySQLConnection


class Binlog:
    def __init__(self, mysql_connection: MySQLConnection):
        self.mysql_connection = mysql_connection

    def show_master_status(self) -> Optional[dict]:
        """
        Returns the MySQL Binlog file name and position

        :return: the database binlog file name and position
        :rtype: dictionary
        """
        binlog_metadata_sql = "SHOW MASTER STATUS"

        logger.trace("Show MySQL Binlog information")

        return self.mysql_connection.fetch_one_query(query=binlog_metadata_sql)

    def first_available_binlog_file_name(self) -> str:
        """
        Returns the first available Binary log file name in MySQL
        :return: The first available Binary log file name
        :rtype: str
        """
        logger.trace("Fetch first available MySQL Binlog file name")

        available_bin_logs_sql = "SHOW BINARY LOGS"

        record = self.mysql_connection.fetch_one_query(query=available_bin_logs_sql)

        if record:
            log_file_name = record["Log_name"]
        else:
            log_file_name = None

        logger.debug(f"First available MySQL Binlog file name: {log_file_name}")

        return log_file_name

    def stream(self, binlog_file_name: str, binlog_position: int, schemas_names: list, tables_names: list,
               server_id: int) -> BinLogStreamReader:
        """
        Creates a MySQL Binlog stream

        :param binlog_file_name: MySQL Binlog file name
        :param binlog_position: MySQL Binlog position
        :param schemas_names: List of schemas names to track changes for
        :param tables_names: List of tables names to track changes for
        :param server_id: The MySQL unique server ID

        :return: MySQL Binlog stream
        :rtype: BinLogStreamReader
        """
        connection_settings = {
            "host": self.mysql_connection.host,
            "port": self.mysql_connection.port,
            "user": self.mysql_connection.username,
            "password": self.mysql_connection.password
        }

        filtered_events = [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RotateEvent, QueryEvent]

        logger.trace(f"Initialize the Binlog stream with {binlog_file_name}:{binlog_position}")

        return BinLogStreamReader(
            connection_settings=connection_settings,
            server_id=server_id,
            only_events=filtered_events,
            log_file=binlog_file_name,
            log_pos=binlog_position,
            resume_stream=True,
            only_schemas=schemas_names,
            only_tables=tables_names,
            slave_heartbeat=100
        )

    @staticmethod
    def latest_snapshot_for_rows(rows: RowsEvent, ignore_columns: list = None) -> list:
        """
        This method returns the latest version of the row. As the MySQL Binlog library returns in case of `UPDATE`
        statement event (`UpdateRowsEvent`) both the recent and the latest version of the row change.

        In case of `INSERT` statement event (`WriteRowsEvent`) or the `DELETE` statement event `DeleteRowsEvent`, the
        library returns only the latest version. It takes into consideration the columns to ignore.

        :param rows: The `RowsEvent` event
        :param ignore_columns: List of columns to ignore
        :return: Latest version for each row in the `RowsEvent` record
        :rtype: list
        """
        ignore_columns = ignore_columns or []

        last_changes = [row["values"] if "values" in row else row["after_values"] for row in rows]

        [change.pop(ignore_column, None) for ignore_column in ignore_columns for change in last_changes]

        return last_changes

    @staticmethod
    def sequence_number(binlog_file_name: str) -> int:
        """
        Extracts the MySQL Binlog sequence number from the Binlog file name. An example MySQL Binlog file name is:
        `mysqlbinlog.001`. Where `001` is the sequence number.

        :param binlog_file_name: Binlog log file name to extract the sequence from
        :return: The sequence number as an integer value
        :rtype: int
        """
        try:
            return int(re.search(r"\b\d+", binlog_file_name).group())
        except TypeError:
            return 0
