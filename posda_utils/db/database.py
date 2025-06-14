# posda_utils/db/database.py

import logging
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from posda_utils.db.models import Base

# SQLite        sqlite:///path/to/file.db
# PostgreSQL    postgresql+psycopg2://user:pass@host:port/dbname
# MySQL         mysql+pymysql://user:pass@host:port/dbname

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, db_url, echo=False):
        self.engine = create_engine(db_url, echo=echo, future=True)
        self.Session = sessionmaker(bind=self.engine, future=True)

    def __enter__(self):
        self.session = self.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.session.rollback()
            else:
                self.session.commit()
        finally:
            self.session.close()
            self.engine.dispose()

    def create_all_tables(self):
        Base.metadata.create_all(self.engine)

    def create_table_from_model(self, model):
        inspector = inspect(self.engine)
        table_name = model.__tablename__

        if table_name not in inspector.get_table_names():
            try:
                model.__table__.create(bind=self.engine)
                logger.info(f"Table '{table_name}' created.")
            except SQLAlchemyError as e:
                logger.error(f"Failed to create table '{table_name}': {e}")

    def run_query(self, query_text, df=False):
        try:
            result = self.session.execute(text(query_text))
            rows = result.fetchall()
            if df:
                return pd.DataFrame(rows, columns=result.keys())
            return rows
        except SQLAlchemyError as e:
            logger.error(f"Query failed: {e}")
            return None

    def run_write(self, query_text, data_dict):
        try:
            self.session.execute(text(query_text), data_dict)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"Write failed: {e}")

    def create_table(self, table_name, columns, schema="public"):
        if not columns:
            raise ValueError("Column definitions must be provided.")

        query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {columns}
            );
        """
        try:
            self.session.execute(text(query))
        except SQLAlchemyError as e:
            logger.error(f"Failed to create table: {e}")

    def truncate_table(self, table_name, schema="public"):
        try:
            self.session.execute(text(f"TRUNCATE TABLE {schema}.{table_name};"))
        except SQLAlchemyError as e:
            logger.error(f"Truncate failed: {e}")