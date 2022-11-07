import csv
import functools
import gzip
from io import BytesIO, TextIOWrapper
from math import ceil

from loguru import logger
from pymysql import OperationalError
from pymysql.cursors import SSCursor, DictCursor

from sources.mysql.connection import Connection as MySQLConnection


class Table:
    HUNDRED_MEGA_BYTES = 100 * 1024 * 1024
    BINARY_DATA_TYPES = ", ".join(["blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary", "geometry"])
    DATE_DATETIME_TIMESTAMP_DATA_TYPES = ", ".join(["datetime", "timestamp", "date"])

    def __init__(self, schema_name: str, table_name: str, initial_replication_where_condition: str = "",
                 ignore_columns: tuple = tuple()):
        self.schema_name = schema_name
        self.table_name = table_name
        self.initial_replication_where_condition = initial_replication_where_condition
        self.ignore_columns = ignore_columns

        self.mysql_connection = None

    def set_mysql_connection(self, mysql_connection: MySQLConnection):
        """
        Sets the MySQL connection

        :param mysql_connection: The `MySQLConnection` object
        """
        self.mysql_connection = mysql_connection

    def buffered_cursor(self) -> DictCursor:
        """
        Returns the MySQL buffered cursor

        :return: The MySQL buffered cursor
        :rtype: DictCursor
        """
        return self.mysql_connection.cursor()

    def unbuffered_cursor(self) -> SSCursor:
        """
        Returns the MySQL unbuffered cursor

        :return: The MySQL unbuffered cursor
        :rtype: SSCursor
        """
        return self.mysql_connection.unbuffered_cursor()

    def export_to_csv(self, max_batch_size_in_bytes: int=HUNDRED_MEGA_BYTES, dry_run: bool=False) -> str:
        """
        This method exports a table to a CSV file. Before exporting the table into a CSV, it applies a read-lock
        on the table so that it prevents any more read on the table when exporting the records which helps in
        replicating the table. It takes into consideration the `WHERE` condition to load the data from MySQL.

        :param max_batch_size_in_bytes: Maximum batch size to fetch from the database and append to CSV
        :param dry_run: A boolean if set to `True` the data will not be written to a local CSV file, only the
            MySQL query will run and the results will be looped over without any actions. This is helpful
            when doing load testing against MySQL. Default is `False` that means the table data are exported to a
            CSV file (compressed CSV file with GZIP).

        :return: A string contains the CSV file path
        :rtype: str
        """
        table_metadata = self.metadata()

        number_of_batches = ceil(table_metadata["total_size_in_bytes"] / max_batch_size_in_bytes)
        logger.debug(f"Fetch table {self.schema_name}.{self.table_name} in {number_of_batches} batch(s)")

        columns_metadata = self.columns_metadata()
        column_casts = [column_metadata["column_cast"] for column_metadata in columns_metadata]
        column_names = [column_metadata["column_name"] for column_metadata in columns_metadata]

        # Execute the SQL statement and export the CSV file
        batch_maximum_number_of_records = round(max_batch_size_in_bytes / (table_metadata["avg_row_length"] or 1.0))
        table_csv_file_path = f"/tmp/{self.schema_name}.{self.table_name}.gzip"

        select_all_from_table = f"""
            SELECT {','.join(column_casts)}
            FROM {self.schema_name}.{self.table_name}
            {self.initial_replication_where_condition}
        """

        unbuffered_cursor = self.unbuffered_cursor()

        unbuffered_cursor.execute(select_all_from_table)

        try:
            if dry_run:
                while True:
                    table_rows = self.mysql_connection.fetchmany(batch_maximum_number_of_records)
                    table_rows_as_dictionary = [dict(zip(column_names, row)) for row in table_rows]

                    if len(table_rows_as_dictionary) == 0:
                        break
            else:
                buffer = BytesIO()

                with gzip.open(table_csv_file_path, mode="wb") as output_file:
                    with TextIOWrapper(output_file, encoding="utf8") as text_wrapper:
                        dict_writer = csv.DictWriter(text_wrapper, column_names)
                        dict_writer.writeheader()

                        while True:
                            table_rows = unbuffered_cursor.fetchmany(batch_maximum_number_of_records)

                            column_names = [column_name[0] for column_name in unbuffered_cursor.description]
                            table_rows_as_dictionary = [dict(zip(column_names, row)) for row in table_rows]

                            if len(table_rows_as_dictionary) == 0:
                                break

                            dict_writer.writerows(table_rows_as_dictionary)

                        output_file.write(buffer.getvalue())

            return table_csv_file_path
        except OperationalError as error:
            logger.error(
                f"MySQL OperationalError in exporting to CSV. {self.schema_name}.{self.table_name}. Error {error}"
            )
        finally:
            unbuffered_cursor.close()

    def lock(self) -> int:
        """
        The method flushes the given table with read lock. It is a safe way to do the replication but not the best
        as it locks the MySQL table until the initial replication is done. This method is not used.

        :return: number of rows the statement affected
        :rtype: int
        """
        logger.debug(f"Apply read-lock on table {self.schema_name}.{self.table_name} in MySQL")

        lock_table_sql = f"FLUSH TABLES {self.schema_name}.{self.table_name} WITH READ LOCK"

        return self.mysql_connection.execute_query(query=lock_table_sql)

    def unlock(self) -> int:
        """
        The method flushes the given table with read lock.

        :return: number of rows the statement affected
        :rtype: int
        """
        logger.debug(f"Remove read-lock on table {self.schema_name}.{self.table_name} in MySQL")

        lock_table_sql = "UNLOCK TABLES"

        return self.mysql_connection.execute_query(query=lock_table_sql)

    def metadata(self) -> dict:
        """
        The method returns the table metadata such as the average row length, number of rows and the total size
        of the table in bytes

        :return: the dictionary contain the metadata
        :rtype: dict
        """
        table_size_in_bytes = f"""
            SELECT avg_row_length, table_rows, table_rows * avg_row_length AS total_size_in_bytes
            FROM information_schema.TABLES 
            WHERE table_type = 'BASE TABLE' 
              AND table_schema = '{self.schema_name}' 
              AND table_name = '{self.table_name}'
        """

        logger.debug(f"Export {self.schema_name}.{self.table_name} metadata from MySQL")

        return self.mysql_connection.fetch_one_query(query=table_size_in_bytes)

    def __columns_metadata_query(self, column_names: tuple=tuple()) -> str:
        """
        This method returns the query to fetch column(s) metadata from MySQL. If there is no specific column passed,
        it returns a query to fetch all columns metadata. The metadata includes: column name, column type,
        default value, nullable and character maximum length. It takes into consideration the columns to ignore.

        :param column_names: Optional, list of column names to return the metadata for

        :return: A query to be executed in MySQL to return the column(s) metadata
        """
        column_filter = f"""AND column_name IN {column_names} """ if column_names else ""
        column_filter += f"AND column_name NOT IN {self.ignore_columns}" if self.ignore_columns else ""

        null_value = "\\" * 5 + "N"

        query = f"""
            SELECT
                column_name,
                data_type,
                column_key,
                column_default,
                is_nullable,
                character_octet_length,
                numeric_precision,
                numeric_scale,
                CASE 
                    WHEN data_type IN ('blob', 'tinyblob', 'mediumblob', 'longblob', 'binary', 'varbinary', 'geometry') 
                        THEN CONCAT('hex(', column_name, ') AS', column_name)
                    WHEN 
                      data_type IN ('bit') THEN CONCAT('CAST(`', column_name, '` AS unsigned) AS ', column_name)
                    WHEN 
                      data_type IN ('date', 'datetime', 'timestamp')
                    THEN
                      CONCAT('NULLIF(`', column_name, '`,"0000-00-00 00:00:00") AS ', column_name)
                    ELSE
                      CONCAT(
                        'IFNULL(',
                        'CAST(`', column_name, '` AS char CHARACTER SET UTF8)',
                        ', ',
                        '"{null_value}"', ') AS ',
                        column_name
                      )
                END
              AS column_cast
            FROM information_schema.columns 
            WHERE table_schema = '{self.schema_name}' AND table_name = '{self.table_name}' {column_filter}
            ORDER BY ordinal_position
        """
        return query

    @functools.lru_cache()
    def columns_metadata(self, column_names: tuple=tuple()) -> list:
        """
        The method returns the columns metadata of a table. The metadata includes the column name and casting
        the column data type to a proper form to be exported simply to CSV

        :return: the dictionary contain the metadata
        :rtype: list
        """
        logger.trace(f"Export {self.schema_name}.{self.table_name} columns metadata from MySQL")

        return self.mysql_connection.fetch_all_query(query=self.__columns_metadata_query(column_names=column_names))

    def column_metadata(self, column_name: str) -> dict:
        """
        This method returns the metadata for a specific column

        :param column_name: Optional, the column name to return the metadata for
        :return: a dictionary, the keys are the metadata of the column and the values are the values.
        """
        logger.debug(f"Export {self.schema_name}.{self.table_name} columns metadata from MySQL")

        return self.mysql_connection.fetch_one_query(
            query=self.__columns_metadata_query(column_names=(column_name, column_name))
        )
