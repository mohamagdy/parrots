import argparse
from collections import defaultdict

import sentry_sdk
from envyaml import EnvYAML
from sentry_sdk import set_tag

from monitoring.mysql_to_singlestore_prometheus_agent import MySQLToSinglestorePrometheusAgent
from replicators.to_singlestore.mysql_to_singlestore.mysql_to_singlestore_replicator import MySQLToSinglestoreReplicator

AVAILABLE_SOURCES = ["mysql"]


def argument_parser() -> argparse.Namespace:
    """
    Parses the program arguments. The passed arguments are:
        - config-path: The path to the YAML configuration file
        - replicate: If passed to the program, the program replicates the tables set in the configuration YAML file
            under the `mysql.sources` field to the destination database
    :return: Arguments object from which the passed arguments could be accessed
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--source", help="The source to replicate data from.", choices=AVAILABLE_SOURCES, required=True)

    parser.add_argument("--config-path", help="Path to YAML configuration file", required=True)
    parser.add_argument(
        "--replicate",
        help="Replicate the tables in the YAML configuration file to the target specified in the YAML file",
        action="store_true"
    )

    parser.add_argument(
        "--stream",
        help="Start the streaming daemon for the tables defined in the YAML configuration file",
        action="store_true"
    )

    parser.add_argument("--tables", help="Comma separated list of tables to replicate", nargs="?")

    parser.add_argument(
        "--writer-sleep-interval-in-seconds", help="Sleep interval for the writer thread",
        default=20, type=int
    )

    parser.add_argument(
        "--dry-run",
        help="""
            Dry run table replication from MySQL without writing the MySQL data to Singlestore or CSV file. Helpful to 
            load test MySQL
        """,
        action="store_true"
    )

    parser.add_argument(
        "--auto-clean-files",
        help="A flag to delete local and S3 files after loading into Singlestore",
        action="store_true"
    )

    parser.add_argument(
        "--keep-existing-data",
        help="""
            A flag to keep the existing data in case full replication of tables is set. This does not have
            effect when full replication `--replicate` argument is set
        """,
        action="store_true"
    )

    return parser.parse_args()


def configuration_parser(yaml_configuration_file_path: str) -> EnvYAML:
    """
    Parses the YAML configuration file
    :param yaml_configuration_file_path: The path to the YAML configuration file
    :return: Dictionary containing the configurations in the YAML file
    :rtype: dict
    """
    return EnvYAML(yaml_configuration_file_path)


if __name__ == "__main__":
    # Parse arguments
    arguments = argument_parser()

    # Parse configurations
    configurations = configuration_parser(yaml_configuration_file_path=arguments.config_path)

    singlestore_config = configurations["singlestore"]
    replication_information_config = configurations["replication_information"]
    sentry_configurations = configurations["sentry"]

    # Initialize Sentry
    sentry_sdk.init(sentry_configurations["dsn"], environment=sentry_configurations["environment"])
    set_tag("schema-name", singlestore_config["target_schema_name"])

    # Monitoring config
    prometheus_config = configurations.get("prometheus")
    prometheus_namespace = prometheus_config.get("namespace")
    prometheus_port = int(prometheus_config.get("port"))

    if arguments.source == "mysql":
        mysql_config = configurations["mysql"]

        prometheus_agent = MySQLToSinglestorePrometheusAgent(prometheus_namespace, prometheus_port)
        prometheus_agent.start_http_server()

        singlestore_replicator = MySQLToSinglestoreReplicator(
            mysql_configurations=mysql_config,
            singlestore_configurations=singlestore_config,
            replication_information_configurations=replication_information_config,
            server_id=mysql_config.get("server_id", 101),
            source_name=mysql_config["source_name"],
            auto_clean_files=arguments.auto_clean_files,
            writer_sleep_interval_in_seconds=arguments.writer_sleep_interval_in_seconds,
            prometheus_agent=prometheus_agent,
            keep_existing_data=arguments.keep_existing_data
        )

        singlestore_replicator.initialize_mysql_tables()
        singlestore_replicator.initialize_singlestore_tables()

        if (arguments.replicate and arguments.tables) or arguments.dry_run:
            if arguments.tables == "ALL":
                tables_to_replicate = singlestore_replicator.mysql_schemas_and_tables
            else:
                # Convert the passed schema and table names in the format <schema>.<table>,<schema>.<table>
                # list into a dictionary the key is the schema name and the value is a list of tables resides in the
                # schema.
                schemas_names_and_tables_names = arguments.tables.split(",")
                tables_to_replicate = defaultdict(defaultdict)

                for schema_name_and_table in schemas_names_and_tables_names:
                    schema_name, table_name = schema_name_and_table.split(".")

                    tables_to_replicate[schema_name].update(
                        {table_name: singlestore_replicator.mysql_schemas_and_tables[schema_name][table_name]}
                    )

            if arguments.replicate:
                singlestore_replicator.replicate_tables(source_schema_and_tables=tables_to_replicate)
            else:
                singlestore_replicator.replicate_tables_dry_run(source_schema_and_tables=tables_to_replicate)

        if arguments.stream:
            singlestore_replicator.stream()

