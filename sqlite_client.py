import sqlite3
from sqlite3 import Error
import pandas as pd


class SQLiteDB:
    def __init__(self, name):
        self.name = name
        self.connection = self.create_connection()
        self.available_import_file_types = {
            "excel": 2,
            "csv": 1,
            "txt": 0
        }
        
    def create_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.name)
            return conn
        except Error as e:
            print(e)
        return conn

    def execute_select_stmt(self, query):
        conn = self.connection
        rows = pd.read_sql_query(query, conn)
        return rows
    
    def execute_select_create_stmt(self, query, tableName, drop_if_exist=True):
        dataframe = self.execute_select_stmt(query)
        ## If tableName already exists... then drop
        if drop_if_exist:
            self.drop_table(tableName)

        dataframe.to_sql(name=tableName, con=self.connection, index=False)
        print(f"Successfully created {tableName}")

    def drop_table(self, table_name):
        conn = self.connection
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS {table_name}')
        print(f'Successfully dropped table {table_name}')
        cur.close()

    def import_file(self, file_path, table_name, file_type="txt"):
        print('Reading file...')

        file_code = self.available_import_file_types.get(file_type, lambda: "Invalid file type")

        if file_code == 0:
            dataframe = pd.read_table(file_path, encoding="ISO-8859-1")
        elif file_code == 1:
            dataframe = pd.read_csv(file_path, encoding="ISO-8859-1")
        elif file_code == 2:
            dataframe = pd.read_excel(file_path)
		
        self.drop_table(table_name)
        dataframe.to_sql(name=table_name, con=self.connection, index=False)
        # Maybe do a count for how many records were inserted
        print(f'Successfully imported {table_name}')

    #specifically pandas df
    def import_table(self, table_data, table_name):
        print('Reading_table')
        dataframe = table_data
        dataframe.to_sql(name=table_name, con=self.connection)
        print('Successfully imported file into table')


