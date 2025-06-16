# posda_utils/db/database.py

import logging
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from posda_utils.db.models import Base

# SQLite        sqlite:///path/to/file.db
# PostgreSQL    postgresql+psycopg2://user:pass@host:port/dbname
# MySQL         mysql+pymysql://user:pass@host:port/dbname

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, conn_string, echo=False, use_single_session=True):
        self.conn_string = conn_string
        self.engine = create_engine(conn_string, echo=echo, future=True)
        self.Session = sessionmaker(bind=self.engine, future=True)
        self.use_single_session = use_single_session
        self.session = None

    def __enter__(self):
        self.session = self.Session() if self.use_single_session else None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.session:
                if exc_type:
                    self.session.rollback()
                else:
                    self.session.commit()
        finally:
            if self.session:
                self.session.close()
            self.engine.dispose()

    @contextmanager
    def _get_session(self):
        if self.use_single_session:
            yield self.session
        else:
            session = self.Session()
            try:
                yield session
            finally:
                session.close()

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

    def run_query(self, query_text, df=False, params=None):
        stmt = text(query_text) if isinstance(query_text, str) else query_text
        with self._get_session() as session:
            try:
                result = session.execute(stmt, params or {})
                rows = result.fetchall()
                return pd.DataFrame(rows, columns=result.keys()) if df else rows
            except SQLAlchemyError as e:
                logger.error(f"Query failed: {e}. Query: {stmt}, Params: {params}")
                return None

    def run_write(self, query_text, data_dict):
        stmt = text(query_text) if isinstance(query_text, str) else query_text
        with self._get_session() as session:
            try:
                session.execute(stmt, data_dict)
                session.commit()
            except SQLAlchemyError as e:
                if self.use_single_session and self.session:
                    self.session.rollback()
                logger.error(f"Write failed: {e}. Query: {stmt}, Params: {data_dict}")

    def create_table(self, table_name, columns, schema="public"):
        if not columns:
            raise ValueError("Column definitions must be provided.")
        if isinstance(columns, (list, tuple)):
            columns = ", ".join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({columns});"
        with self._get_session() as session:
            try:
                session.execute(text(query))
            except SQLAlchemyError as e:
                logger.error(f"Failed to create table: {e}. Query: {query}")

    def truncate_table(self, table_name, schema="public"):
        query = f"TRUNCATE TABLE {schema}.{table_name};"
        with self._get_session() as session:
            try:
                session.execute(text(query))
            except SQLAlchemyError as e:
                logger.error(f"Truncate failed: {e}. Query: {query}")
