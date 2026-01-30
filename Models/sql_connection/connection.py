import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import logging
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_engine = None

def get_engine(db_url):
    global _engine
    if _engine is None:
        _engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
    return _engine

def read_sql_query(file_path):
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
        return sql_query
    except FileNotFoundError:
        logger.error(f"SQL file not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading SQL file {file_path}: {str(e)}")
        raise

@contextmanager
def get_connection(db_url):
    engine = get_engine(db_url)
    try:
        with engine.connect() as conn:
            yield conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def execute_query(sql_file_path, db_url, params=None):
    try:
        sql_query = read_sql_query(sql_file_path)
        logger.info(f"Executing query from {sql_file_path}")
        
        with get_connection(db_url) as conn:
            df = pd.read_sql(sql_query, conn, params=params)
            logger.info(f"Query executed successfully. Returned {len(df)} rows")
            return df
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return pd.DataFrame()

def execute_sql_query(sql_query, db_url, params=None):
    try:
        logger.info(f"Executing SQL query: {sql_query}")
        
        with get_connection(db_url) as conn:
            result = conn.execute(text(sql_query), params or {})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Query executed successfully. Returned {len(df)} rows")
            return df
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return pd.DataFrame()

def execute_sql(sql, db_url, params=None):
    try:
        # logger.info(f"Executing SQL: {sql}")
        
        with get_connection(db_url) as conn:
            conn.execute(text(sql), params)
            conn.commit()
            logger.info("SQL executed successfully.")
    except Exception as e:
        logger.error(f"Error executing SQL: {str(e)}")
        raise