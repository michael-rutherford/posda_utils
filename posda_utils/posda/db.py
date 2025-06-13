# posda_utils/posda/db.py

import pandas as pd
import hashlib
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pydicom
from pydicom import dcmread


class PosdaDB:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.engine:
            self.engine.dispose()
    
    def __init__(self, conn_data, db):
        pydicom.config.convert_wrong_length_to_UN = True
        self.engine = create_engine(
            f'postgresql+psycopg2://{conn_data["un"]}:{conn_data["pw"]}@{conn_data["host"]}:{conn_data["port"]}/{db}',
            pool_recycle=3600
        )
        self.Session = sessionmaker(bind=self.engine)

    def get_connection(self):
        return self.engine.connect()

    def test_connection(self):
        try:
            with self.get_connection() as conn:
                print("Connection successful")
        except SQLAlchemyError as e:
            print(f"Connection error: {e}")

    def run_query(self, query_text, df=False):
        try:
            query = text(query_text)
            with self.get_connection() as conn:
                result = conn.execute(query)
                rows = result.fetchall()
                if df:
                    columns = result.keys()
                    return pd.DataFrame(rows, columns=columns)
                else:
                    return rows
        except SQLAlchemyError as e:
            print(f"Query Failed: {e}")
            return None
        
    def run_write(self, query_text, data_dict):
        try:
            query = text(query_text)
            with self.engine.begin() as conn:
                conn.execute(query, data_dict)
        except SQLAlchemyError as e:
            print(f"Write failed: {e}")

    def create_table(self, table_name, schema="public", columns=None):

        if columns is None:
            raise ValueError("Column definitions must be provided.")
    
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {columns}
            );
        """

        try:
            with self.get_connection() as conn:                
                with conn.begin():
                    conn.execute(text(create_query))
            print(f"Table {schema}.{table_name} created (if not exists).")
        except SQLAlchemyError as e:
            print(f"Error creating table {table_name}: {e}")

    def truncate_table(self, table_name, schema="public"):

        truncate_query = f"TRUNCATE TABLE {schema}.{table_name};"
        
        try:
            with self.get_connection() as conn:
                with conn.begin():
                    conn.execute(text(truncate_query))
            print(f"Table {schema}.{table_name} truncated.")
        except SQLAlchemyError as e:
            print(f"Error truncating table {table_name}: {e}")

    # Posda functions

    def get_recent_timepoint(self, file_id):
        query = f"""
            select max(atf.activity_timepoint_id) 
            from activity_timepoint_file atf 
            where atf.file_id = {file_id}
        """
        result = self.run_query(query)
        return result[0][0] if result else None
    
    def get_timepoint_files(self, tp):
        query = f"""
            select atf.file_id 
            from activity_timepoint_file atf 
            where atf.activity_timepoint_id = {tp}
        """
        result = self.run_query(query)
        return [row[0] for row in result] if result else []
