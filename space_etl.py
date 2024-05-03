from sqlalchemy import create_engine, text
from sqlalchemy.schema import CreateSchema
import psycopg2

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

    def query(self, query):
        with self.engine.connect() as conn:
            cur = conn.execute(text(query))
            return cur

    def write_dataframe(self,df,details):
        table_name = details['table_name']
        schema_name = details['schema_name']
        # check if table exists
        # if exists add column
        # then load
        #if not exists columns
        # then load
        df.to_sql(table_name,schema = schema_name, con = self.engine, if_exists = 'append',index = False)
        
    def close_connection(self):
        return self.engine.dispose()
        
class SchemaDriftHandle:
    def __init__(self,db_name) -> None:
        self.db_name = db_name
        self.db_user_name = 'tsandil'
        self.db_user_password = 'stratocaster'
        self.conn = psycopg2.connect(
            database = f'{self.db_name}',
            user = f'{self.db_user_name}',
            password= f'{self.db_user_password}',
            host = '127.0.0.1',
            port = '5432'
        )

    def execute_query(self,query):
        try:
            cur = self.conn.cursor()
            cur.execute(query=query)
            self.conn.commit()
            print(f'Query {query} Executed Successfully.\n')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error Excuting the query {query}\n: {e}')
        return cur
        

    def add_columns(self,details,column_name,column_type):
        schema_name = details['schema_name']
        table_name = details['table_name']
        query = f'ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column_name} {column_type}'
        self.execute_query(query=query)
        print(f"Added column {column_name} of type {column_type} to the table.")

    def check_schema_drift(self,df,details):
        schema_name = details['schema_name']
        table_name = details['table_name']
        df_columns = df.columns.tolist()
        query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
        cur = self.execute_query(query=query)
        
        existing_columns = [row[0] for row in cur.fetchall()]

        columns_to_add = [col for col in df_columns if col not in existing_columns]
        if columns_to_add:
            for column_name in columns_to_add:
                column_type = 'VARCHAR(255)'
                self.add_columns(details=details,column_name=column_name,column_type=column_type)
        pass

                
