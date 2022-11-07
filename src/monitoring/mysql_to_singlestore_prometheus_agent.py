from monitoring.base_prometheus_agent import BasePrometheusAgent


class MySQLToSinglestorePrometheusAgent(BasePrometheusAgent):
    # Constants
    LAST_SOURCE_TRANSACTION_TIMESTAMP_KEY = "last_source_transaction_timestamp_in_seconds"
    LAST_SOURCE_TRANSACTION_TIMESTAMP_DESCRIPTION = "Timestamp in seconds since last source transaction"

    MYSQL_BINLOG_FILE_NAME_KEY = "last_mysql_binlog_file_name"
    MYSQL_BINLOG_FILE_NAME_DESCRIPTION = "The last processed MySQL Binlog filename"

    MYSQL_BINLOG_POSITION_IN_FILE = "last_mysql_binlog_position_in_file"
    MYSQL_BINLOG_POSITION_DESCRIPTION = "The last processed MySQL Binlog position in MySQL Binlog file"

    METADATA_UPDATE_SECONDS_KEY = "metadata_update_seconds"
    METADATA_UPDATE_SECONDS_DESCRIPTION = "Time in seconds spent updating the metadata table"

    def set_last_source_transaction_timestamp(self, value: int, schema_name: str, table_name: str):
        """
        Sets the `last_source_transaction_timestamp_in_seconds` gauge

        :param value: The value to be set
        :param schema_name: The name of the schema related to that value
        :param table_name: The name of the table related to that value
        """
        if self.LAST_SOURCE_TRANSACTION_TIMESTAMP_KEY not in self.gauges:
            self.add_gauge_key(
                self.LAST_SOURCE_TRANSACTION_TIMESTAMP_KEY,
                self.LAST_SOURCE_TRANSACTION_TIMESTAMP_DESCRIPTION,
                self.LABELS
            )

        self.set_gauge_key(self.LAST_SOURCE_TRANSACTION_TIMESTAMP_KEY, value, schema_name, table_name)

    def set_metadata_update_seconds(self, value: int):
        """
        Sets the `metadata_update_seconds` gauge

        :param value: The value to be set
        """
        if self.METADATA_UPDATE_SECONDS_KEY not in self.gauges:
            self.add_gauge_key(self.METADATA_UPDATE_SECONDS_KEY, self.METADATA_UPDATE_SECONDS_DESCRIPTION, [])

        self.set_gauge_key(self.METADATA_UPDATE_SECONDS_KEY, value)


