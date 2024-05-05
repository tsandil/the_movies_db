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

    # def query(self, query):
    #     with self.engine.connect() as conn:
    #         cur = conn.execute(text(query))
    #         return cur
    
    def execute_query(self,query):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                conn.commit()
                return result
        except Exception as e:
            print(f"Error executing SQL query: {e}")
            return None

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
        
