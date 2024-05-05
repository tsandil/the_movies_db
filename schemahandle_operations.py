from space_etl import PostgresqlDestination


class SchemaDriftHandle(PostgresqlDestination):
    def __init__(self, db_name) -> None:
        super().__init__(db_name)
    
    def select_existing_columns(self,details):
        db_name = 'my_database1'
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f"SELECT column_name FROM  information_schema.columns WHERE  table_schema = '{schema_name}' AND table_name = '{table_name}'"

        select_cols = PostgresqlDestination(db_name = db_name)
        cur = select_cols.execute_query(query=query)
        result = cur.fetchall()
        return result
    
    def check_schema_drift(self,df,details):
        with self.engine.connect() as conn:

            # Checking if column exists or no
            df_columns = df.columns.tolist()
            cur = self.select_existing_columns(details=details)

            existing_columns = [row[0] for row in cur]
            columns_to_add = [col for col in df_columns if col not in existing_columns]

            # # Checking for the data type Schema Drift
            # df_columns_datattype = df.dtypes
            # existing_table_datatypes = self.get_column_datatypes(details=details)

            # existing_table_dtypes_dict = dict(existing_table_datatypes)

            # print(df_columns_datattype)
            # print("------asdasd-------\n")
            # print(existing_table_dtypes_dict)

            # for col_name,df_dtype in df_columns_datattype.items():
            #     if col_name in existing_table_dtypes_dict:
            #         table_datatype = existing_table_dtypes_dict[col_name]

            #         if str(df_dtype) != table_datatype:
            #             new_data_type = SchemaDriftHandle.map_df_dtype_to_postgres(df_dtype)
            #             self.alter_column_datatypes(details=details,column_name=col_name,column_type=new_data_type)
            return columns_to_add    

    def add_columns(self, details,column_name, column_type):
        db_name = 'my_database1'
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column_name} {column_type}"
        
        add_cols = PostgresqlDestination(db_name=db_name)
        cur = add_cols.execute_query(query=query)
        print(f"Added column {column_name} of type {column_type} to the table.")
        return cur
    
    def alter_column_datatypes(self,details,column_name,column_type):
        db_name = details['db_name']
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f"ALTER TABLE {schema_name}.{table_name} ALTER COLUMN {column_name} TYPE {column_type}"
        alter_cols = PostgresqlDestination(db_name=db_name)
        cur = alter_cols.execute_query(query=query)
        print(f"Column : {column_name} has been altered to type {column_type}")
        return cur

    def get_column_datatypes(self,details):
        db_name = details['db_name']
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f"SELECT column_name, data_type FROM information_schema. columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'; "
        col_dtype = PostgresqlDestination(db_name=db_name)
        cur = col_dtype.execute_query(query=query)
        result = cur.fetchall()
        print(result)
        return result
    

def map_df_dtype_to_postgres(df_dtype):
    if df_dtype == 'int64':
        return 'integer'
    elif df_dtype == 'float64':
        return 'numeric'
    elif df_dtype =='object':
        return 'text'
    else:
        return 'text'