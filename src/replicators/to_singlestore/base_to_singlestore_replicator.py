import functools
from collections import defaultdict

import singlestoredb
from singlestoredb.connection import Connection as SinglestoreConnection

from monitoring.base_prometheus_agent import BasePrometheusAgent
from sinks.singlestore.positions_table import PositionsTable
from sinks.singlestore.table import Table as SinglestoreTable


class BaseToSinglestoreReplicator:
    def __init__(self, singlestore_configurations: dict, replication_information_configurations: dict,
                 auto_clean_files: bool, writer_sleep_interval_in_seconds: int, prometheus_agent: BasePrometheusAgent,
                 keep_existing_data: bool):
        self.singlestore_configurations = singlestore_configurations
        self.replication_information_configurations = replication_information_configurations

        self.auto_clean_files = auto_clean_files

        self.writer_sleep_interval_in_seconds = writer_sleep_interval_in_seconds

        self.keep_existing_data = keep_existing_data

        self.prometheus_agent = prometheus_agent

        self.singlestore_tables = defaultdict(SinglestoreTable)

    @property
    def singlestore_schema_name(self) -> str:
        """
        Returns the target Singlestore schema name
        :return: String represents the Singlestore target schema name
        :rtype: str
        """
        return self.singlestore_configurations["target_schema_name"]

    @property
    def singlestore_tables_schemas(self) -> dict:
        """
        Returns the schemas for the Singlestore tables.

        :return: Dictionary containing the table schemas. The key is the table name and the value is a list of
            dictionaries, the key is the column name and the value if the column type.
        :rtype: dict
        """
        return self.singlestore_configurations["table_schemas"]

    @property
    @functools.lru_cache()
    def singlestore_connection(self) -> SinglestoreConnection:
        """
        Creates a Singlestore database connection.

        :return: `SinglestoreConnection` object
        :rtype: SinglestoreConnection
        """
        return singlestoredb.connect(
            host=self.singlestore_configurations["host"],
            port=self.singlestore_configurations["port"],
            user=self.singlestore_configurations["username"],
            password=self.singlestore_configurations["password"],
            local_infile=True,
            autocommit=False,
            results_format="dict"
        )

    @property
    def replication_schema_name(self) -> str:
        """
        Returns the replication schema name where the coordinates of the replication is stored in.

        :return: the replication schema name where the coordinates of the replication is stored in
        :rtype: str
        """
        return self.replication_information_configurations["schema_name"]

    @property
    def replication_positions_table_name(self) -> str:
        """
        Returns the replication table name where the coordinates of the replication is stored in.

        :return: the replication table name where the coordinates of the replication is stored in
        :rtype: str
        """
        return self.replication_information_configurations["positions_table_name"]

    @property
    @functools.lru_cache()
    def replicator_positions_table(self) -> PositionsTable:
        """
        Creates or return the replication table used to store replicated tables coordinates in MySQL Binlog
        :return: The `positions` table that used to store the replicated tables coordinates in MySQL Binlog
        :rtype: PositionsTable
        """
        positions_table = PositionsTable(
            connection=self.singlestore_connection,
            schema_name=self.replication_schema_name,
            table_name=self.replication_positions_table_name
        )

        positions_table.create_if_not_exists()

        return positions_table
