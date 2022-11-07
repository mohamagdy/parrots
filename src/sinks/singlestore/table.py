import functools

from loguru import logger
from singlestoredb.connection import Connection as SinglestoreConnection


class Table:
    HUNDRED_MEGA_BYTES = 100 * 1024 * 1024

    def __init__(self, connection: SinglestoreConnection, schema_name: str, table_name: str):
        self.connection = connection
        self.cursor = self.connection.cursor()

        self.schema_name = schema_name
        self.table_name = table_name

    @property
    def full_name(self) -> str:
        """
        Returns the full name (schema and table name)

        :return: The full name with a period separated. For example, `schema.table`
        :rtype: str
        """
        return f"{self.schema_name}.{self.table_name}"

    @property
    @functools.lru_cache()
    def primary_keys(self) -> list:
        """
        Returns the primary keys of the table and cache the value.

        :return: List of primary key(s) of the table
        :rtype: list
        """
        primary_keys_sql = f"""
            SELECT sta.column_name
            FROM information_schema.tables AS tab
            JOIN information_schema.statistics AS sta ON sta.table_schema = tab.table_schema 
                AND sta.table_name = tab.table_name
                AND sta.index_name = 'primary'
            WHERE  tab.table_schema = '{self.schema_name}' AND tab.table_name = '{self.table_name}'
                   AND tab.table_type = 'BASE TABLE'; 
        """

        logger.debug(f"Fetch primary key(s) for {self.schema_name}.{self.table_name} in Singlestore")

        self.cursor.execute(primary_keys_sql)
        primary_keys = self.cursor.fetchall()
        self.connection.commit()

        return [key["column_name"] for key in primary_keys]

    @property
    @functools.lru_cache()
    def not_nulls(self) -> list:
        """
        Returns the columns which are not nullable.

        :return: List of columns which are not nullable
        :rtype: list
        """
        not_nulls_sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{self.schema_name}' AND table_name   = '{self.table_name}' AND is_nullable = 'NO';
        """

        logger.debug(f"Fetch NOT NULL columns for {self.schema_name}.{self.table_name} in Singlestore")

        self.cursor.execute(not_nulls_sql)
        not_nulls = self.cursor.fetchall()
        self.connection.commit()

        return [not_null[0] for not_null in not_nulls]

    @property
    @functools.lru_cache()
    def columns(self) -> list:
        """
        Returns the columns of the table.

        :return: List of columns
        :rtype: list
        """
        list_columns = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{self.schema_name}' AND table_name   = '{self.table_name}'
            ORDER BY ordinal_position;
        """

        logger.debug(f"Fetch all columns for {self.schema_name}.{self.table_name} in Redshift")

        self.cursor.execute(list_columns)
        list_columns = self.cursor.fetchall()
        self.connection.commit()

        return [column["column_name"] for column in list_columns]

    @property
    def insert_sql_template(self) -> str:
        """
        Template for the `INSERT` statement to be performed in Redshift syntax

        :return: SQL template for `INSERT` statement
        :rtype: sql.SQL
        """
        return f"""
            INSERT INTO {self.schema_name}.{self.table_name} (%s)
            VALUES %s
        """

    @property
    def delete_sql_template(self) -> str:
        """
        Template for the `DELETE` statement to be performed in Redshift syntax

        :return: SQL template for `DELETE` statement
        :rtype: sql.SQL
        """
        return f"""
            DELETE FROM {self.schema_name}.{self.table_name}
            WHERE %s
        """

    @property
    def update_sql_template(self) -> str:
        """
        Template for the `UPDATE` statement to be performed in Redshift syntax

        :return: SQL template for `UPDATE` statement
        :rtype: sql.SQL
        """
        return f"""
            UPDATE {self.schema_name}.{self.table_name}
            SET %s
            WHERE %s
        """

    @property
    def truncate_sql_template(self) -> str:
        """
        Template for the `TRUNCATE` statement to be performed in Redshift syntax

        :return: SQL template for `TRUNCATE` statement
        :rtype: sql.SQL
        """
        return f"TRUNCATE TABLE {self.schema_name}.{self.table_name}"

    @property
    def delete_using_staging_table_sql_template(self) -> str:
        """
        Template for the `DELETE ... JOIN` statement to be performed in Redshift syntax

        :return: SQL template for `DELETE ... JOIN` statement
        :rtype: sql.SQL
        """
        condition = " AND ".join(
            f"{self.table_name}.{primary_key_name.lower()} = staging.{primary_key_name.lower()}"
            for primary_key_name in self.primary_keys
        )

        return f"""
            DELETE {self.table_name}
            FROM {self.schema_name}.{self.table_name}
            JOIN {self.staging_schema_name}.{self.table_name} AS staging
            WHERE {condition}
        """

    @property
    def staging_schema_name(self):
        """
        Returns the schema where the staging tables created.

        :return: The schema where the staging tables created
        :rtype: str
        """
        return f"{self.schema_name}_staging"

    def create_if_not_exists(self, columns_names_and_types: dict, primary_keys: list = None,
                             create_staging_table: bool = True):
        """
        Creates a table in SingleStore if does not exist

        :param columns_names_and_types: A dictionary, the keys are the column names and values is the data type,
            character length and nullability of the column.
        :param primary_keys: List of primary keys of the table. Default no empty set.
        :param create_staging_table: A boolean if set to `True` create a staging table in the staging schema

        :return: Returns `True` if the `CREATE` command is successful otherwise it returns `False`.
        :rtype: bool
        """
        primary_keys = primary_keys or []

        columns_definition = ",\n".join(
            f"{column_name} {column_type}" for column_name, column_type in columns_names_and_types.items()
        )

        primary_key_definition = f", PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
                {columns_definition}
                {primary_key_definition}
            );
        """

        logger.debug(f"Create table {self.schema_name}.{self.table_name} in Redshift")

        self.cursor.execute(create_table_sql)
        self.connection.commit()

        if create_staging_table:
            self.create_staging_table_if_not_exists()

        return True

    def create_staging_table_if_not_exists(self) -> bool:
        """
        Creates the staging table for the table.

        :return: Returns `True` if the `CREATE` command is successful otherwise it returns `False`.
        :rtype: bool
        """
        create_staging_table_if_not_exists_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.staging_schema_name}.{self.table_name}
            (LIKE {self.schema_name}.{self.table_name});
        """

        logger.debug(f"Create table {self.staging_schema_name}.{self.table_name} in Redshift")

        self.cursor.execute(create_staging_table_if_not_exists_statement)
        self.connection.commit()

        return True

    def truncate(self) -> bool:
        """
        Truncates the table.

        :return: Returns `True` if the `TRUNCATE` command is successful otherwise it returns `False`.
        :rtype: bool
        """
        truncate_table_sql = self.truncate_sql_template

        logger.debug(f"Truncate table {self.schema_name}.{self.table_name} in Redshift")

        self.cursor.execute(truncate_table_sql)
        self.connection.commit()

        return True

    def truncate_staging_table(self) -> bool:
        """
        Truncates the staging table.

        :return: Returns `True` if the `TRUNCATE` command is successful otherwise it returns `False`.
        :rtype: bool
        """
        truncate_table_sql = f"TRUNCATE TABLE {self.staging_schema_name}.{self.table_name}"

        logger.debug(f"Truncate staging table {self.staging_schema_name}.{self.table_name} in Redshift")

        self.cursor.execute(truncate_table_sql)
        self.connection.commit()

        return True

    def drop_if_exists(self, drop_staging_table: bool = True) -> bool:
        """
        Drops the final table.

        :param drop_staging_table: A boolean is set to `True` to drop the staging table. If set to `False` it skips
            dropping the staging table. Default is `True`.

        :return: Returns `True` if the `DROP` command for only the main table is successful otherwise
            it returns `False`.
        :rtype: bool
        """
        drop_table_sql = f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}"

        logger.debug(f"Drop table {self.schema_name}.{self.table_name}")

        self.cursor.execute(drop_table_sql)

        if drop_staging_table:
            self.drop_staging_table_if_exists()

        return True

    def drop_staging_table_if_exists(self):
        """
        Drops the staging table.
        """
        drop_table_sql = f"DROP TABLE IF EXISTS {self.staging_schema_name}.{self.table_name}"

        logger.debug(f"Drop table {self.staging_schema_name}.{self.table_name}")

        self.cursor.execute(drop_table_sql)
        self.connection.commit()

        return True

    def load_file(self, path: str, to_staging_table: bool = False) -> bool:
        """
        Imports the passed CSV file path to the table.

        :param path: The path of the CSV file to import into the table
        :param to_staging_table: Boolean set to `True` if the import to be to the staging table

        :return: Returns `True` if the `COPY` command is successful otherwise returns `False`.
        :rtype: bool
        """
        if to_staging_table:
            self.truncate_staging_table()
            schema_name = self.staging_schema_name
        else:
            self.truncate()
            schema_name = self.schema_name

        load_file_sql = f"""
            LOAD DATA LOCAL INFILE '{path}' COMPRESSION GZIP
            INTO TABLE {schema_name}.{self.table_name}
            FIELDS TERMINATED BY ','
            IGNORE 1 LINES
        """

        logger.debug(f"Import CSV into table {schema_name}.{self.table_name} in Redshift")

        self.cursor.execute(load_file_sql)
        self.connection.commit()

        return True

    def insert(self, columns_names_and_values: dict):
        """
        Inserts a new record to the table. The row to be inserted ins passed as a dictionary. The key is the column name
        and the value is the column value.

        :param columns_names_and_values: A dictionary, the key is the column name and the value is the value of the
            column

        :return: A boolean if the `INSERT` operation is successful otherwise it returns `False`.
        :rtype: bool
        """
        column_names = [column.lower() for column in columns_names_and_values.keys()]
        column_values = [columns_names_and_values[column] for column in columns_names_and_values.keys()]

        logger.debug(f"Insert row into {self.schema_name}.{self.table_name}")

        insert_sql = self.insert_sql_template % (", ".join(column_names), tuple(column_values))

        self.cursor.execute(insert_sql)
        self.connection.commit()

        return True

    def update(self, columns_names_and_values: dict) -> bool:
        """
        Updates an existing record in the table.

        :param columns_names_and_values: A dictionary, the key is the column name and the value is the value of the
            column. The list contains both the changes columns and the primary key values

        :return: A boolean if the `UPDATE` operation is successful and the table has primary key(s). Otherwise
            it returns `False`.
        :rtype: bool
        """
        primary_keys = dict(
            (column_name.lower(), columns_names_and_values[column_name]) for column_name in self.primary_keys
        )

        if primary_keys and all(primary_keys.values()):  # Skip records having `None` values for primary keys
            settings = []
            settings_values = []
            for column_name, column_value in columns_names_and_values.items():
                if column_name not in primary_keys:
                    settings.append(f"{column_name.lower()} = %s")
                    settings_values.append(str(column_value) if column_value is not None else "NULL")

            condition = " AND ".join(
                f"{column_name} = '{column_value}'" for column_name, column_value in primary_keys.items()
            )

            logger.debug(f"Update row in {self.schema_name}.{self.table_name} {primary_keys}")

            update_sql = self.update_sql_template % (", ".join(settings), condition)

            self.cursor.execute(update_sql, params=tuple(settings_values))
            self.connection.commit()

            return True
        else:
            logger.warning("Skip updating record due to missing primary keys!")

            return False

    def delete(self, columns_names_and_values: dict) -> bool:
        """
        Deletes an existing record from the table

        :param columns_names_and_values: A dictionary, the key is the primary key name and the value is the value of the
            primary key

        :return: A boolean if the `DELETE` operation is successful and the table has primary key(s). Otherwise
            it returns `False`.
        :rtype: bool
        """
        if columns_names_and_values:
            logger.debug(f"Delete row from {self.schema_name}.{self.table_name} {columns_names_and_values}")

            condition = " AND ".join(
                f"{column_name.lower()} = '{column_value}'"
                for column_name, column_value in columns_names_and_values.items()
            )

            delete_sql = self.delete_sql_template % (condition, )

            self.cursor.execute(delete_sql)
            self.connection.commit()

            return True
        else:
            logger.warning("Skip deleting record due to missing primary keys!")

            return False

    def delete_using_staging_table(self) -> bool:
        """
        Deletes records from the final table using the records in the staging table. This is called after the data
        file is loaded into the staging table.

        :return: A boolean `True` if there is no errors occurred otherwise `False`.
        :rtype: bool
        """
        logger.debug(f"Delete from {self.full_name} using staging table")

        self.cursor.execute(self.delete_using_staging_table_sql_template)
        self.connection.commit()

        return True

    def update_or_insert_using_staging_table(self) -> bool:
        """
        Apply `UPSERT` operation to the final table using the staging table.

        :return: A boolean `True` if there is no errors occurred otherwise `False`.
        :rtype: bool
        """
        logger.debug(f"Update or insert {self.full_name} using staging table")

        self.cursor.execute(f"{self.delete_using_staging_table_sql_template};")
        self.cursor.execute(f"INSERT INTO {self.schema_name}.{self.table_name} SELECT * FROM {self.staging_schema_name}.{self.table_name};")
        self.connection.commit()

        return True

    def replicate_using_staging_table(self, delete_condition: str) -> bool:
        """
        Replicate to the final table using the staging table. The cleanup of the final table is done before inserting
        from the `staging` table. This helps in keeping existing data that does not match the given `delete_condition`.
        All the data matching the `delete_condition` will be deleted and replaced by fresh data from the staging table.

        :return: A boolean `True` if there is no errors occurred otherwise `False`.
        :rtype: bool
        """
        logger.debug(f"Replicate to {self.full_name} using staging table")

        self.cursor.execute(f"DELETE FROM {self.schema_name}.{self.table_name} {delete_condition or ''};")
        self.cursor.execute(f"INSERT INTO {self.schema_name}.{self.table_name} SELECT * FROM {self.staging_schema_name}.{self.table_name};")
        self.connection.commit()

        return True

    def alter(self, action: str, column_name: str, column_type=None) -> bool:
        """
        This method executes and `ALTER` command against the Redshift database. It only executes the column addition,
        dropping and modifying actions. It runs the `ALTER` command against both the final table and the staging
        table.

        :param action: The actions to be executed either: ADD, DROP or MODIFY/CHANGE
        :param column_name: The column name to add, drop or modify/change
        :param column_type: The new data type of the column

        :return: A boolean if the `ALTER` command is successful otherwise return `False`.
        :rtype: bool
        """
        for schema_name in [self.schema_name, self.staging_schema_name]:
            base_query = f"ALTER TABLE {schema_name}.{self.table_name}"

            if action == "ADD":
                alert_table_query = f"{base_query} ADD COLUMN {column_name} {column_type}"

                logger.info(f"Add column {column_name} to {schema_name}.{self.table_name} in Redshift")
            elif action in ["MODIFY", "CHANGE"]:
                only_column_type = column_type.split(" ")[0]
                alert_table_query = f"{base_query} ALTER COLUMN {column_name} TYPE {only_column_type}"

                logger.info(f"Modify column {column_name} in {schema_name}.{self.table_name} in Redshift")
            else:
                alert_table_query = f"{base_query} DROP COLUMN {column_name}"

                logger.info(f"Drop column {column_name} from {schema_name}.{self.table_name} in Redshift")

            self.cursor.execute(alert_table_query)
            self.connection.commit()

            return True
