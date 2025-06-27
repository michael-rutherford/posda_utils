# posda_utils/db/database.py

import logging
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import io
import psycopg2
from psycopg2.extras import execute_values
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

    def bulk_insert(self, df, table, schema="public", if_exists="append", index=False):
        if df.empty:
            return

        try:
            df.to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=index,
                method="multi"
            )
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            raise

    # PostgreSQL ONLY - COPY method for bulk insert
    def copy_from_df(self, df, table, schema="public"):
        if df.empty:
            return

        # Create a CSV buffer (PostgreSQL COPY format)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        full_table_name = f"{schema}.{table}"

        with self.engine.raw_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.copy_expert(f"COPY {full_table_name} FROM STDIN WITH CSV", buffer)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"COPY insert failed: {e}")
                    raise

    # PostgreSQL ONLY - Bulk update using UPDATE ... FROM (VALUES ...)
    def bulk_update(self, rows, target_table, key_column, update_columns, schema="public", batch_size=1000):
        """
        Perform bulk UPDATE using PostgreSQL's UPDATE ... FROM (VALUES ...) syntax.

        :param rows: List of tuples like [(key, col1, col2, ...), ...]
        :param target_table: Table to update.
        :param key_column: Column used for matching (e.g. 'origin_file_id').
        :param update_columns: List of columns to update (e.g. ['modality', 'series']).
        :param schema: Schema name.
        :param batch_size: How many rows to process at once.
        """

        if not rows:
            return

        total_cols = [key_column] + update_columns
        col_placeholders = ', '.join(total_cols)
        set_clause = ', '.join([f"{col} = v.{col}" for col in update_columns])
        full_table_name = f"{schema}.{target_table}"

        with self.engine.begin() as connection:
            raw_conn = connection.connection
            with raw_conn.cursor() as cur:
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    execute_values(
                        cur,
                        f"""
                        UPDATE {full_table_name} AS t
                        SET {set_clause}
                        FROM (VALUES %s) AS v({col_placeholders})
                        WHERE t.{key_column} = v.{key_column}
                        """,
                        batch
                    )
