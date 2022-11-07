class MySQLToSinglestoreConverter:
    @staticmethod
    def convert_mysql_table_to_singlestore(mysql_columns_metadata: list) -> list:
        """
        This method converts a table definition from MySQL syntax to SingleStore syntax

        :param mysql_columns_metadata: A list of dictionaries for the MySQL table column names, data type,
            nullability and primary key (metadata)

        :return: A dictionary containing 2 keys:
            - `columns_names_and_type` which is a list of dictiniaries
                key is the column name and the value if the column data type in Singlestore together with the character
                length and nullability.

            - `primary_keys` list of primary keys of the table

        :rtype: dict
        """
        columns_names_and_type = {}
        primary_keys = []
        for column_metadata in mysql_columns_metadata:
            column_name = column_metadata["column_name"]
            column_key = column_metadata["column_key"]

            if column_key == "PRI":
                primary_keys.append(column_name)

            columns_names_and_type[column_name.lower()] = \
                MySQLToSinglestoreConverter.convert_mysql_column_to_singlestore(column_metadata)

        return [columns_names_and_type, primary_keys]

    @staticmethod
    def convert_mysql_column_to_singlestore(column_metadata: dict) -> str:
        """
        Converts the given MySQL metadata of a column to the corresponding Singlestore metadata

        :param column_metadata: the column metadata, this includes: name, type, nullable, default value and
            character length
        :return: A string contains the Singlestore metadata of the column
        """
        data_type = column_metadata["data_type"]
        nullable = column_metadata["is_nullable"] == "YES"
        character_octet_length = column_metadata["character_octet_length"]
        precision = column_metadata["numeric_precision"]
        scale = column_metadata["numeric_scale"]

        singlestore_column_type = MySQLToSinglestoreConverter.column_type_converter(data_type)

        nullable_or_not_nullable = "NULL" if nullable else "NOT NULL"

        if not nullable:
            default_value = column_metadata["column_default"] or \
                            MySQLToSinglestoreConverter.default_value_for_type(singlestore_column_type)
            default_value = f"DEFAULT '{default_value}'"
        else:
            default_value = ""

        if character_octet_length and singlestore_column_type != "text":
            singlestore_column_type = f"{singlestore_column_type}({character_octet_length})"
        else:
            if singlestore_column_type == "decimal":
                singlestore_column_type = f"{singlestore_column_type}({precision}, {scale})"

        return f"{singlestore_column_type} {nullable_or_not_nullable} {default_value}"

    @staticmethod
    def column_type_converter(data_type: str) -> str:
        """
        Converts the given MySQL data type to a Singlestore data type

        :param data_type: MySQL data type
        :return: Singlestore corresponding data type
        :rtype: str
        """
        return {
            "integer": "integer",
            "mediumint": "integer",
            "tinyint": "integer",
            "smallint": "integer",
            "int": "integer",
            "bigint": "bigint",
            "varchar": "varchar",
            "character varying": "character varying",
            "text": "text",
            "char": "character",
            "datetime": "timestamp",
            "date": "date",
            "time": "time without time zone",
            "timestamp": "timestamp",
            "tinytext": "text",
            "mediumtext": "text",
            "longtext": "text",
            "tinyblob": "bytea",
            "mediumblob": "bytea",
            "longblob": "bytea",
            "blob": "bytea",
            "binary": "bytea",
            "varbinary": "bytea",
            "decimal": "decimal",
            "double": "double precision",
            "double precision": "double precision",
            "float": "double precision",
            "bit": "integer",
            "year": "integer",
            "enum": "text",
            "set": "text",
            "json": "json",
            "bool": "boolean",
            "boolean": "boolean",
            "geometry": "bytea",
        }[data_type]

    @staticmethod
    def default_value_for_type(data_type: str) -> object:
        zero_default_value = ["integer", "bigint", "decimal", "double precision"]
        boolean_value = ["boolean"]
        text_value = ["varchar", "character varying", "text", "character"]
        datetime_value = ["time without time zone", "timestamp"]
        date_value = ["date"]

        if data_type in zero_default_value:
            return 0
        elif data_type in boolean_value:
            return "FALSE"
        elif data_type in text_value:
            return ""
        elif data_type in datetime_value:
            return "1970-01-01 00:00:00"
        elif data_type in date_value:
            return "1970-01-01"
        else:
            return ""
