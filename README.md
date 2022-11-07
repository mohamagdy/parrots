# Parrots

## Introduction

The parrots project is the near real-time replication Python daemon that copies data from different sources such as MySQL to different destinations such as SingleStore.

## Technical Details

To run the Parrots daemon make sure to follow those steps:

1. Install the requirements using `pip install -r requirements.txt`
1. Create the destination schemas on SingleStore. There are 3 schemas should be created:
    1. The destination schema itself. For example `target`
    1. The staging schema at which `parrots` writes the staging data into. It should be named as `<destination-schema>_staging`. For example if the destination schema named `target` then the staging schema should be `target_staging`
    1. The metadata schema. The metadata schema contains the metadata tables that stores the `parrots` metadata such as the last successfully processed MySQL Binlog file name, MySQL Binlog position in file per table. For example, name the schema `replication`.
1. The following environment variables needs to be exported:

    | Environment Variable Name | Value                                                                                                                                                                                                                               |
    |---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | MYSQL_HOST                | The MySQL source host                                                                                                                                                                                                               |
    | MYSQL_PORT                | The MySQL source port. For example `3306`                                                                                                                                                                                           |
    | MYSQL_USERNAME            | The MySQL source username                                                                                                                                                                                                           |
    | MYSQL_PASSWORD            | The MySQL source password                                                                                                                                                                                                           |
    | MYSQL_SCHEMA_NAME         | The MySQL source schema from which the data will be read from                                                                                                                                                                       |
    | SINGLESTORE_HOST          | The SingleStore destination host                                                                                                                                                                                                    |
    | SINGLESTORE_PORT          | The SingleStore destination port                                                                                                                                                                                                    |
    | SINGLESTORE_USERNAME      | The SingleStore destination username                                                                                                                                                                                                |
    | SINGLESTORE_PASSWORD      | The SingleStore destination password                                                                                                                                                                                                |
    | SINGLESTORE_TARGET_SCHEMA | The SingleStore destination schema to which the replicated data will be written to                                                                                                                                                  |
    | REPLICATION_SCHEMA        | The name of the schema that will store the metadata table. For example `replication`                                                                                                                                                |
    | REPLICATION_TABLE         | The name of the metadata table that stores the last MySQL Binlog file and position for each table. For example `positions`                                                                                                          |
    | PROMETHEUS_PORT           | The Prometheus port. For example 9090                                                                                                                                                                                               |
    | PROMETHEUS_NAMESPACE      | The Prometheus namespace. For example `parrots`                                                                                                                                                                                     |
    | LOGURU_LEVEL              | The log level                                                                                                                                                                                                                       |
    | LOGURU_FORMAT             | The log format. For example `<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> \| <red>{thread.name}</red> \| <level>{level: <8}</level> \| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>` |
    | SENTRY_ENVIRONMENT        | The Sentry environment for tracking exceptions. For example `DEBUG`                                                                                                                                                                 |
    | SENTRY_DSN                | The Sentry DSN. Could be left empty if there is no Sentry project                                                                                                                                                                   |

1. Run the `parrots` daemon as follows:
     ```shell script
    $ cd src
    $ PYTHONPATH='.' python mains/singlestore.py 
       --config-path ../config/mysql/test.yml \
       --replicate --stream --tables ALL \
       --source mysql
    ```

