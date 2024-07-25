from sqlalchemy import create_engine, text

class PostgresqlDestination:

    #Creating a Constructor to create the connection for the Load phase.
    def __init__(self,db_name) -> None:
        self.db_name = db_name
        self.db_user_name = 'tsandil'
        self.db_user_password = 'stratocaster'
        self.engine = create_engine(f'postgresql://{self.db_user_name}:{self.db_user_password}@127.0.0.1:5432/{self.db_name}')

    def create_schema(self,schema_name):
        with self.engine.connect() as conn:
            query = f"create schema if not exists {schema_name};"
            conn.execute(text(query))
            conn.commit()
            return conn


    
    def execute_query(self,query):
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            conn.commit()
            return result
        
    def write_dataframe(self,df,details):
        table_name = details['table_name']
        schema_name = details['schema_name'] 
        schema_handle = SchemaDriftHandle(db_name=self.db_name)

        # Checking if schema exists/not.
        schema_exists = schema_handle.check_schema_exists(details=details)
        if not schema_exists:
            self.create_schema(schema_name=schema_name)
        
        # Checking if table exists in the schema or not.
        table_exists = schema_handle.check_table_exists(details=details)
        print(f"Table exists{table_exists}")
        if not table_exists:
            schema_handle.create_table(df=df,details=details)

        print('\n\nChecking schema drift')
        columns_to_add,modified_cols = schema_handle.check_schema_drift(df=df,details=details)
        print(f"\n\nCols to add \n\n{columns_to_add}")
        print(f"\n\nCols to modify \n\n{modified_cols}")

        if columns_to_add or modified_cols:
            schema_handle.handle_schema_drift(df=df,details=details,columns_to_add=columns_to_add,modified_cols=modified_cols)

        df.to_sql(table_name,schema = schema_name, con = self.engine, if_exists = 'append',index = False)
        
    def close_connection(self):
        return self.engine.dispose()
        

class SchemaDriftHandle(PostgresqlDestination):
    def __init__(self, db_name) -> None:
        super().__init__(self)
        super().__init__(db_name)
    

    def check_schema_exists(self,details):
        schema_name = details['schema_name']
        query = f"SELECT EXISTS (SELECT * FROM pg_catalog.pg_namespace where nspname = '{schema_name}');"
        cur = self.execute_query(query=query)
        result = cur.fetchone()[0]
        return result
    
    def check_table_exists(self,details):
        table_name = details['table_name']
        schema_name = details['schema_name']
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}');"
        cur = self.execute_query(query=query)
        result = cur.fetchone()[0]
        return result
    
    def create_table(self,df,details):
        table_name = details['table_name']
        schema_name = details['schema_name']
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
        query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns_str});"

        self.execute_query(query=query)
        print(f"Table with name {table_name} created...")
        


    

    def select_existing_columns(self,details):
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f"SELECT column_name FROM  information_schema.columns WHERE  table_schema = '{schema_name}' AND table_name = '{table_name}'"

        cur = self.execute_query(query=query)
        result = cur.fetchall()
        return result
    
    def drop_table(self,table_name,schema_name):
        query = f"DROP TABLE {schema_name}.{table_name}"
        self.execute_query(query=query)


    def get_column_info(self,table_name,schema_name):
        query = f"SELECT column_name,data_type FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"

        cur = self.execute_query(query=query)
        result = cur.fetchall()
        return result
    
    def check_schema_drift(self,df,details):
        with self.engine.connect() as conn:
            table_name = details['table_name']
            schema_name = details['schema_name']
            dest_column_info = self.get_column_info(table_name=table_name,schema_name=schema_name)
            df.to_sql("temp_tab",schema = schema_name,con = self.engine,index=False)
            source_column_info = self.get_column_info(table_name='temp_tab', schema_name=schema_name)
            self.drop_table(table_name='temp_tab', schema_name=schema_name)

            dest_col = {col_name:dtype for col_name,dtype in dest_column_info}
            source_col = {col_name:dtype for col_name,dtype in source_column_info}

            columns_to_add = []
            modified_cols = []

            # Checking for Schema Drift for added cols in data frame.
            for col_name in source_col:
                if col_name not in dest_col:
                    columns_to_add.append(col_name)
            
            # Checking for Schema Drift for modified cols in dataframe
            for key in source_col:
                if key in dest_col and dest_col[key]!=source_col[key]:
                    modified_cols.append(key)



            print(f"\n\n\n Destination Column Info : \n\n{dest_col}")

            print(f"\n\n\n source Column Info : \n\n{source_col}")
            return columns_to_add,modified_cols
        
  
    def handle_schema_drift(self,df,details,columns_to_add,modified_cols):
        for col_name in columns_to_add:
            df_datatype = df[col_name].dtype
            col_type = self.map_df_dtype_to_postgres(df_datatype)
            self.add_columns(details=details,column_name=col_name,column_type=col_type)


        existing_columns = [col[0] for col in self.select_existing_columns(details=details)]
        for col_name in modified_cols:
            df_datatype = df[col_name].dtype
            new_column_name = f'{col_name}_new_{df_datatype}'
            
            if new_column_name.lower() not in existing_columns:
                col_type = self.map_df_dtype_to_postgres(df_datatype)
                self.add_columns(details=details, column_name=new_column_name, column_type=col_type)
            else:
                print(f"Column {new_column_name} already exists. Using the existing column.")
            
            df.rename(columns = {col_name:new_column_name}, inplace = True)













        
    def add_columns(self, details,column_name, column_type):
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column_name} {column_type};"
        
        cur = self.execute_query(query=query)
        print(f"Added column {column_name} of type {column_type} to the table.")
        return cur
    
    def alter_column_datatypes(self,details,column_name,column_type):
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f"ALTER TABLE {schema_name}.{table_name} ALTER COLUMN {column_name} TYPE {column_type}"
        cur = self.execute_query(query=query)
        print(f"Column : {column_name} has been altered to type {column_type}")
        return cur

    def get_column_datatypes(self,details):
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f"SELECT column_name, data_type FROM information_schema. columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'; "
        cur = self.execute_query(query=query)
        result = cur.fetchall()
        #print(result)
        return result
    
    # def handle_schema_drift(self,df, details,columns_to_add,changed_datatype_columns):
    #         for column_name in columns_to_add:
    #             df_datatype = df[column_name].dtype
    #             column_type = self.map_df_dtype_to_postgres(df_datatype)
    #             self.add_columns(details=details,column_name=column_name,column_type=column_type)
            


    #         existing_columns = self.select_existing_columns(details=details)
    #         existing_columns = [col[0] for col in existing_columns]

    #         for column_name in changed_datatype_columns:
    #             df_datatype = df[column_name].dtype
    #             new_column_name = f'{column_name}_new_{df_datatype}'
                
    #             if new_column_name.lower() not in existing_columns:
    #                 column_type=self.map_df_dtype_to_postgres(df_datatype)
    #                 self.add_columns(details=details,column_name=new_column_name,column_type=column_type)
    #             else:
    #                 print(f"Column {new_column_name} already exists. Using the existing column.")
                    
                

    #             df[new_column_name] = df[column_name].astype(str)





    

    def map_df_dtype_to_postgres(self,df_dtype):
        dtype_map = {
            'int8':'SMALLINT',
            'int16':'SMALLINT',
            'int32':'INTEGER',
            'int64':'BIGINT',
            'float16':'REAL',
            'float32':'REAL',
            'float64':'DOUBLE PRECISION',
            'bool':'BOOLEAN',
            'datetime64[ns]':'TIMESTAMP',
            'string':'TEXT',
            'boolean':'BOOLEAN',
            'datetime':'TIMESTAMP',
            'date':'DATE',
            'float':'DOUBLE PRECISION',
            'integer':'BIGINT',
            'arrays':'ARRAY',
            'datetime64':'TIMESTAMP',
            'object':'TEXT',
            'category':'TEXT',
            'mixed':'TEXT',
        }

        dtype_str = str(df_dtype)
        column_type = dtype_map.get(dtype_str,'TEXT')

        return column_type
    

    





