from contextlib import contextmanager
from datetime import datetime
from typing import Callable, Optional

from loguru import logger
from prometheus_client import start_http_server, REGISTRY, PROCESS_COLLECTOR, PLATFORM_COLLECTOR, Gauge


class BasePrometheusAgent:
    # Constants
    LABELS = ["schema_name", "table_name"]

    INITIAL_REPLICATION_STARTED_KEY = "initial_replication_started"
    INITIAL_REPLICATION_STARTED_DESCRIPTION = "Initial tables replication started"

    STREAMING_STARTED_KEY = "streaming_started"
    STREAMING_STARTED_DESCRIPTION = "Streaming changes started"

    INSERT_OR_UPDATE_SECONDS_KEY = "insert_or_update_seconds"
    INSERT_OR_UPDATE_SECONDS_DESCRIPTION = "Time in seconds for the update and/or inserts to/into the table"

    DELETE_SECONDS_KEY = "delete_seconds"
    DELETE_SECONDS_DESCRIPTION = "Time in seconds for the deletes from the table"

    TOTAL_TABLE_FLUSH_TIME_SECONDS_KEY = "total_table_flush_time_seconds"
    TOTAL_TABLE_FLUSH_TIME_SECONDS_DESCRIPTION = "Total time spent flushing the table to destination"

    REDSHIFT_OPEN_CONNECTIONS_KEY = "open_redshift_connections"
    REDSHIFT_OPEN_CONNECTIONS_DESCRIPTION = "Number of Redshift connections open"

    LAST_DESTINATION_TRANSACTION_TIMESTAMP_KEY = "last_destination_transaction_timestamp_in_seconds"
    LAST_DESTINATION_TRANSACTION_TIMESTAMP_DESCRIPTION = "Timestamp in seconds of the last destination transaction"

    LAST_CAUGHT_EXCEPTION_TIMESTAMP_KEY = "last_caught_exception_timestamp_in_seconds"
    LAST_CAUGHT_EXCEPTION_TIMESTAMP_DESCRIPTION = "Timestamp in seconds of the last caught exception"

    def __init__(self, namespace: str, port: int):
        self.port = port
        self.namespace = namespace

        self.gauges = dict()

    def start_http_server(self):
        """
        Starts the Prometheus server on the given port
        """
        logger.info(f"Starting Prometheus server on port {self.port}")

        # Unregistering the Python Garbage Collector, process and platform metrics
        REGISTRY.unregister(PROCESS_COLLECTOR)
        REGISTRY.unregister(PLATFORM_COLLECTOR)

        for collector_name in list(REGISTRY._collector_to_names.keys()):
            REGISTRY.unregister(collector_name)

        start_http_server(self.port)

    @contextmanager
    def time_it(self, metrics_method: Callable[[int, Optional[str], Optional[str]], None], message: str = None, **args):
        start_time = datetime.now()
        yield
        elapsed_time = (datetime.now() - start_time).seconds

        if message:
            logger.debug(f"{message} took {elapsed_time} seconds")

        metrics_method(elapsed_time, **args)

    def set_initial_replication_started(self):
        """
        Sets the `streaming_started` gauge when the application started
        """
        if self.INITIAL_REPLICATION_STARTED_KEY not in self.gauges:
            self.add_gauge_key(self.INITIAL_REPLICATION_STARTED_KEY, self.INITIAL_REPLICATION_STARTED_DESCRIPTION, [])

        self.set_gauge_current_timestamp(self.INITIAL_REPLICATION_STARTED_KEY)

    def set_streaming_started(self):
        """
        Sets the `initial_replication_started` gauge when the application started
        """
        if self.STREAMING_STARTED_KEY not in self.gauges:
            self.add_gauge_key(self.STREAMING_STARTED_KEY, self.STREAMING_STARTED_DESCRIPTION, [])

        self.set_gauge_current_timestamp(self.STREAMING_STARTED_KEY)

    def set_update_or_insert_seconds(self, value: int, schema_name: str, table_name: str):
        """
        Sets the `insert_or_update_seconds` gauge

        :param value: The value to be set
        :param schema_name: The name of the schema related to that value
        :param table_name: The name of the table related to that value
        """
        if self.INSERT_OR_UPDATE_SECONDS_KEY not in self.gauges:
            self.add_gauge_key(
                self.INSERT_OR_UPDATE_SECONDS_KEY, self.INSERT_OR_UPDATE_SECONDS_DESCRIPTION, self.LABELS
            )

        self.set_gauge_key(self.INSERT_OR_UPDATE_SECONDS_KEY, value, schema_name, table_name)

    def set_delete_seconds(self, value: int, schema_name: str, table_name: str):
        """
        Sets the `delete_seconds` gauge

        :param value: The value to be set
        :param schema_name: The name of the schema related to that value
        :param table_name: The name of the table related to that value
        """
        if self.DELETE_SECONDS_KEY not in self.gauges:
            self.add_gauge_key(self.DELETE_SECONDS_KEY, self.DELETE_SECONDS_DESCRIPTION, self.LABELS)

        self.set_gauge_key(self.DELETE_SECONDS_KEY, value, schema_name, table_name)

    def set_total_table_flush_time_seconds(self, value: int, schema_name: str, table_name: str):
        """
        Sets the `total_table_flush_time_seconds` gauge

        :param value: The value to be set
        :param schema_name: The name of the schema related to that value
        :param table_name: The name of the table related to that value
        """
        if self.TOTAL_TABLE_FLUSH_TIME_SECONDS_KEY not in self.gauges:
            self.add_gauge_key(
                self.TOTAL_TABLE_FLUSH_TIME_SECONDS_KEY, self.TOTAL_TABLE_FLUSH_TIME_SECONDS_DESCRIPTION, self.LABELS
            )

        self.set_gauge_key(self.TOTAL_TABLE_FLUSH_TIME_SECONDS_KEY, value, schema_name, table_name)

    def set_open_redshift_connections(self, value: int):
        """
        Sets the `open_redshift_connections` gauge

        :param value: The value to be set
        """
        if self.REDSHIFT_OPEN_CONNECTIONS_KEY not in self.gauges:
            self.add_gauge_key(self.REDSHIFT_OPEN_CONNECTIONS_KEY, self.REDSHIFT_OPEN_CONNECTIONS_DESCRIPTION, [])

        self.set_gauge_key(self.REDSHIFT_OPEN_CONNECTIONS_KEY, value)

    def set_last_destination_transaction_timestamp(self, schema_name: str, table_name: str):
        """
        Sets the `last_destination_transaction_timestamp_in_seconds` gauge

        :param schema_name: The name of the schema related to that value
        :param table_name: The name of the table related to that value
        """
        if self.LAST_DESTINATION_TRANSACTION_TIMESTAMP_KEY not in self.gauges:
            self.add_gauge_key(
                self.LAST_DESTINATION_TRANSACTION_TIMESTAMP_KEY,
                self.LAST_DESTINATION_TRANSACTION_TIMESTAMP_DESCRIPTION,
                self.LABELS
            )

        self.set_gauge_current_timestamp(self.LAST_DESTINATION_TRANSACTION_TIMESTAMP_KEY, schema_name, table_name)

    def set_last_caught_exception_timestamp(self, schema_name: str = "-", table_name: str = "-"):
        """
        Sets the `last_destination_caught_exception_timestamp_in_seconds` gauge

        :param schema_name: The name of the schema related to that value
        :param table_name: The name of the table related to that value
        """
        if self.LAST_CAUGHT_EXCEPTION_TIMESTAMP_KEY not in self.gauges:
            self.add_gauge_key(
                self.LAST_CAUGHT_EXCEPTION_TIMESTAMP_KEY,
                self.LAST_CAUGHT_EXCEPTION_TIMESTAMP_DESCRIPTION,
                self.LABELS
            )

        self.set_gauge_current_timestamp(self.LAST_CAUGHT_EXCEPTION_TIMESTAMP_KEY, schema_name, table_name)

    # Gauge
    def add_gauge_key(self, key_name: str, key_description: str, label_names: list):
        """
        Adds a key with type `Gauge` to the list of gauges

        :param key_name: The key name to be added
        :param key_description: The key description
        :param label_names: List of label names. Default is `None`
        """
        self.gauges[key_name] = Gauge(
            name=key_name,
            documentation=key_description,
            namespace=self.namespace,
            labelnames=label_names
        )

    def set_gauge_key(self, key_name: str, value: int, *labels):
        """
        Sets a given key with type `Gauge`

        :param key_name: The key name
        :param value: The value to be set
        :param labels: Dictionary of labels, the key is the label name and the value is the label value
        """
        gauge = self.gauges[key_name]

        if len(labels):
            gauge = gauge.labels(*labels)

        gauge.set(value)

    def set_gauge_current_timestamp(self, key_name: str, *labels):
        """
        Sets the value a given key with type `Gauge` to the current timestamp

        :param key_name: The key name
        :param labels: Dictionary of labels, the key is the label name and the value is the label value
        """
        gauge = self.gauges[key_name]

        if len(labels):
            gauge = gauge.labels(*labels)

        gauge.set_to_current_time()
