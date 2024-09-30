import json
import time
from sqlalchemy import text
from .queries import QUERIES
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresqlDestination:

    # Creating a Constructor to create the connection for the Load phase.
    def __init__(self, db_name) -> None:
        self.db_name = db_name
        self.db_user_name = "tsandil"
        self.db_user_password = "stratocaster"

        postgres_hook = PostgresHook(postgres_conn_id="themovies_con")
        self.engine = postgres_hook.get_sqlalchemy_engine(
            engine_kwargs={"future": True}
        )

    def create_schema(self, schema_name):
        """
        This function creates schema in Postgres
        Args:
            schema_name: Takes name of schema
        Returns:
            conn:
        """
        with self.engine.connect() as conn:
            query = f"create schema if not exists {schema_name};"
            conn.execute(text(query))
            conn.commit()
            return conn

    def merge_tables(self, details):
        """

        This function merges the two tables i.e. Destination Table and temp table to ensure that no duplicate data is inserted into the Destination Table.
        Takes the Merge query from utilities.queries.py

        """

        table_name = details["table_name"]
        dest_table = details["dest_table"]
        schema_name = details["schema_name"]
        schema_handle = SchemaDriftHandle(db_name=self.db_name)
        columns_info = schema_handle.get_column_info(
            table_name=table_name, schema_name=schema_name
        )
        column_names = [col["column_name"] for col in columns_info]

        # Create the dynamic parts for the query
        column_definitions = ", ".join(
            [f"{col['column_name']} {col['data_type']}" for col in columns_info]
        )
        insert_columns = ", ".join(column_names)
        values_columns = ", ".join([f"t2.{col}" for col in column_names])
        update_cond = " OR ".join(
            [f"t1.{col} != t2.{col} OR (t1.{col} is null and t2.{col} is not null) OR (t1.{col} is not null and t2.{col} is null)\n" for col in column_names if col!="record_loaded_at"]
        )
        update_columns = ", ".join(
            [f"{col} = t2.{col}" for col in column_names]
        )

        print(f"\n\nThese are the updated-columns query part {update_columns}")

        create_table = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{dest_table} ({column_definitions});"""
        self.execute_query(query=create_table)

        print(f"\ncheck this : \n{update_cond}")

        merge_query = f"{QUERIES['merge_to_table']}".format(
            schema_name=schema_name,
            dest_table=dest_table,
            table_name=table_name,
            insert_columns=insert_columns,
            values_columns=values_columns,
            update_columns=update_columns,
            update_cond = update_cond,
        )
        print(f"final \n {merge_query}")
        self.execute_query(query=merge_query)

    def execute_query(self, query):
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            conn.commit()
            return result

    def write_dataframe(self, df, details):
        """
        This function loads the pandas dataframe into the PostgreSQL table.
        This functions checks, if the necessary schema,table exists. Also, checks for structural schema drift and  handles accordingly.

        """
        table_name = details["table_name"]
        schema_name = details["schema_name"]

        schema_handle = SchemaDriftHandle(db_name=self.db_name)

        # Checking if schema exists/not.
        schema_exists = schema_handle.check_schema_exists(details=details)
        if not schema_exists:
            self.create_schema(schema_name=schema_name)

        # Checking if table exists in the schema or not.
        table_exists = schema_handle.check_table_exists(details=details)
        print(f"Table {table_name} exists: {table_exists}")
        if not table_exists:
            # This will create table on its own and load to temp table
            df.drop_duplicates(inplace=True)
            df.to_sql(
                table_name,
                schema=schema_name,
                con=self.engine,
                if_exists="append",
                index=False,
            )

            # Merging data to the main table
            self.merge_tables(details=details)
            print("Data Merged")

            # return _response

        print("\n\nChecking schema drift")
        columns_to_add, modified_cols = schema_handle.check_schema_drift(
            df=df, details=details
        )
        print(f"\n\nCols to add \n\n{columns_to_add}")
        print(f"\n\nCols to modify \n\n{modified_cols}")

        if columns_to_add or modified_cols:
            schema_handle.handle_schema_drift(
                df=df,
                details=details,
                columns_to_add=columns_to_add,
                modified_cols=modified_cols,
            )

        df.drop_duplicates(inplace=True)
        df.to_sql(
            table_name,
            schema=schema_name,
            con=self.engine,
            if_exists="append",
            index=False,
        )
        # Merging data to the main table
        self.merge_tables(details=details)
        print("Data Merged")

        schema_handle.drop_table(table_name=table_name, schema_name=schema_name)

        self.close_connection()

    def close_connection(self):
        return self.engine.dispose()


class SchemaDriftHandle(PostgresqlDestination):
    def __init__(self, db_name) -> None:
        super().__init__(self)
        super().__init__(db_name)

    def check_schema_exists(self, details):
        schema_name = details["schema_name"]
        query = f"SELECT EXISTS (SELECT * FROM pg_catalog.pg_namespace where nspname = '{schema_name}');"
        cur = self.execute_query(query=query)
        result = cur.fetchone()[0]
        return result

    def check_table_exists(self, details):
        table_name = details["table_name"]
        schema_name = details["schema_name"]
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}');"
        cur = self.execute_query(query=query)
        result = cur.fetchone()[0]
        return result

    def create_table(self, df, details):
        table_name = details["table_name"]
        schema_name = details["schema_name"]
        print(table_name)
        print(schema_name)
        df_col_datatypes = df.dtypes

        columns = []
        for df_colname, df_dtype in df_col_datatypes.items():
            # here, we create a column definition to pass to sql query
            postgres_type = self.map_df_dtype_to_postgres(df_dtype=df_dtype)
            column_def = f"{df_colname} {postgres_type}"
            columns.append(column_def)

        columns_str = ", ".join(columns)
        query = (
            f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns_str});"
        )

        self.execute_query(query=query)
        print(f"Table with name {table_name} created...")

    def select_existing_columns(self, details):
        schema_name = details["schema_name"]
        table_name = details["table_name"]
        query = f"SELECT column_name FROM  information_schema.columns WHERE  table_schema = '{schema_name}' AND table_name = '{table_name}'"

        cur = self.execute_query(query=query)
        result = cur.fetchall()
        return result

    def drop_table(self, table_name, schema_name):
        query = f"DROP TABLE {schema_name}.{table_name}"
        self.execute_query(query=query)

    def get_column_info(self, table_name, schema_name):
        query = f"{QUERIES['get_column_properites']}".format(
            schema_name=schema_name, table_name=table_name
        )

        cur = self.execute_query(query=query)
        result = cur.fetchall()
        column_properties = [json.loads(response[0]) for response in result]
        return column_properties

    def check_schema_drift(self, df, details):
        with self.engine.connect() as conn:
            table_name = details["table_name"]
            schema_name = details["schema_name"]
            timestamp = int(time.time())
            temp_table = f"temp_{table_name}_{timestamp}"

            dest_column_info = self.get_column_info(
                table_name=table_name, schema_name=schema_name
            )

            df.to_sql(temp_table, schema=schema_name, con=conn, index=False)

            source_column_info = self.get_column_info(
                table_name=temp_table, schema_name=schema_name
            )

            self.drop_table(table_name=temp_table, schema_name=schema_name)

            print(source_column_info)

            dest_col_names = [col["column_name"] for col in dest_column_info]

            print(f"\n\n Destination Columns: \n{dest_col_names}")

            columns_to_add = []
            modified_cols = []

            # Checking for Schema Drift for added cols in data frame.
            for values in source_column_info:
                if values["column_name"] not in dest_col_names:
                    columns_to_add.append(values)

                # Checking for Schema Drift for modified cols in dataframe
                if values["column_name"] in dest_col_names:
                    if (
                        values["data_type"]
                        != dest_column_info[
                            dest_col_names.index(values["column_name"])
                        ]["data_type"]
                    ):
                        modified_cols.append(values)

            return columns_to_add, modified_cols

    def handle_schema_drift(self, df, details, columns_to_add, modified_cols):
        schema_name = details["schema_name"]
        table_name = details["table_name"]
        dest_table = details["dest_table"]
        if columns_to_add:
            for data in columns_to_add:
                self.add_columns(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=data["column_name"],
                    column_type=data["data_type"],
                )
                self.add_columns(
                    schema_name=schema_name,
                    table_name=dest_table,
                    column_name=data["column_name"],
                    column_type=data["data_type"],
                )

        if modified_cols:
            for data in modified_cols:
                sql1 = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN IF NOT EXISTS {data['column_name']}_{str(data['data_type']).replace(' ', '_')} {data['data_type']}"
                cur = self.execute_query(query=sql1)

                sql2 = f"ALTER TABLE {schema_name}.{dest_table} ADD COLUMN IF NOT EXISTS {data['column_name']}_{str(data['data_type']).replace(' ', '_')} {data['data_type']}"
                cur = self.execute_query(query=sql2)

                df.rename(
                    columns={
                        data[
                            "column_name"
                        ]: f"{data['column_name']}_{str(data['data_type']).replace(' ', '_')}"
                    },
                    inplace=True,
                )
                return cur

    def add_columns(self, schema_name, table_name, column_name, column_type):
        query = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column_name} {column_type};"

        cur = self.execute_query(query=query)
        print(f"Added column {column_name} of type {column_type} to the table.")
        return cur
