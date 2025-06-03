from typing import Dict, List, Literal, Optional
import boto3
from io import BytesIO, StringIO
import sys
from sqlalchemy import create_engine, text
import logging
import pandas as pd
import pymysql
from ..config import RDS_HOST, NAME, PASSWORD, TEKKX_SCALABLE_DB_NAME, YUGIOH_DB, DB_PORT


# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def save_df_to_mysql(df: pd.DataFrame, table_name: str, if_exists="replace") -> None:
    _, engine = get_engine_for_tekkx_scalable_db(
        db_name="yugioh_data")  # or your actual DB
    try:
        df.to_sql(table_name, con=engine, index=False,
                  if_exists=if_exists, method='multi')  # type: ignore
        print(f"✅ Successfully uploaded to MySQL table: {table_name}")
    except Exception as e:
        print(f"❌ Error uploading to MySQL: {e}")


def save_df_to_s3(df: pd.DataFrame, bucket_name: str, dir: str, filename_for_backup: str) -> None:
    """
    Save a pandas DataFrame to an S3 bucket as a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        bucket_name (str): The name of the S3 bucket.
        dir (str): The directory path inside the bucket.
        filename_for_backup (str): The filename for the backup CSV file.

    Raises:
        Exception: If the S3 upload fails.
    """
    try:
        buffer = BytesIO()
        df.to_csv(buffer, index=False, encoding="utf-8")
        buffer.seek(0)

        client = boto3.client('s3')
        client.put_object(Body=buffer.getvalue(),
                          Bucket=bucket_name, Key=f"{dir}{filename_for_backup}")
        logger.info(
            f"DataFrame successfully uploaded to S3: {bucket_name}/{dir}{filename_for_backup}")
    except Exception as e:
        logger.error(f"Failed to save DataFrame to S3: {e}")
        raise


def save_to_s3(bucket_name: str, object_key: str, body_buffer: StringIO, file_type: str = "json") -> None:
    """
    Save a StringIO buffer to an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The S3 object key (file path).
        body_buffer (StringIO): The buffer containing data to be uploaded.
        file_type (str): The type of the file ('json' or 'csv').

    Raises:
        Exception: If the S3 upload fails.
    """
    try:
        s3_client = boto3.client('s3')
        body = body_buffer.getvalue().encode(
            'utf-8') if file_type == "json" else body_buffer.getvalue()

        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body)
        logger.info(
            f"File successfully uploaded to S3: {bucket_name}/{object_key}")
    except Exception as e:
        logger.error(f"Failed to upload file to S3: {e}")
        raise


def upload_data(df: pd.DataFrame, table_name: str, if_exist: Literal['fail', 'replace', 'append'], db_name: Optional[str] = TEKKX_SCALABLE_DB_NAME) -> None:
    """
    Upload a pandas DataFrame to a MySQL database.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        table_name (str): The target table name.
        if_exist (Literal['fail', 'replace', 'append']): Behavior if the table exists.
        db_name (str): The name of the database.

    Raises:
        Exception: If the data upload fails.
    """
    try:
        engine = create_engine(
            f"mysql+pymysql://{NAME}:{PASSWORD}@{RDS_HOST}:{DB_PORT}/{db_name}")
        print("engine", engine)
        df.to_sql(con=engine, name=table_name, if_exists=if_exist, index=False)
        logger.info(
            f"DataFrame successfully uploaded to {db_name}.{table_name}")
    except Exception as e:
        logger.error(f"Failed to upload DataFrame to database: {e}")
        raise


def upload_data_v2(df: pd.DataFrame, table_name: str, if_exist: Literal['fail', 'replace', 'append'], db_name: Optional[str] = TEKKX_SCALABLE_DB_NAME) -> None:
    """
    Upload a pandas DataFrame to a MySQL database.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        table_name (str): The target table name.
        if_exist (Literal['fail', 'replace', 'append']): Behavior if the table exists.
        db_name (str): The name of the database.

    Raises:
        Exception: If the data upload fails.
    """
    try:
        engine = create_engine(
            f"mysql+pymysql://{NAME}:{PASSWORD}@{RDS_HOST}:{DB_PORT}/{db_name}")
        df.to_sql(con=engine, name=table_name, if_exists=if_exist, index=False)
        logger.info(
            f"DataFrame successfully uploaded to {db_name}.{table_name}")
    except Exception as e:
        logger.error(f"Failed to upload DataFrame to database: {e}")
        raise


def get_engine_for_tekkx_scalable_db(db_name: str = TEKKX_SCALABLE_DB_NAME):
    """
    Create and return a SQLAlchemy engine for the TekkX scalable database.

    Args:
        db_name (str): The name of the database to connect to.

    Returns:
        tuple: A logger instance and a SQLAlchemy engine.

    Raises:
        Exception: If the connection to the database fails.
    """
    db_uri = f"mysql+pymysql://{NAME}:{PASSWORD}@{RDS_HOST}:{DB_PORT}/{db_name}"

    try:
        engine = create_engine(db_uri, echo=True)
        logger.info(f"Connected to the database: {db_name}")
        return logger, engine
    except pymysql.MySQLError as e:
        logger.error("ERROR: Could not connect to the MySQL database.")
        logger.error(e)
        sys.exit()


def retrieve_data_from_db_to_df(table_name: str, db_name: str = TEKKX_SCALABLE_DB_NAME) -> pd.DataFrame:
    """
    Retrieve data from a MySQL database and load it into a pandas DataFrame.

    Args:
        table_name (str): The name of the table to retrieve data from.
        db_name (str): The name of the database.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved data.

    Raises:
        Exception: If the data retrieval fails.
    """
    logger, engine = get_engine_for_tekkx_scalable_db(db_name)

    try:
        with engine.begin() as conn:
            df = pd.read_sql_query(
                sql=text(f"SELECT * FROM {table_name}"), con=conn)
            logger.info(f"Data retrieved from {db_name}.{table_name}")
            return df
    except Exception as e:
        logger.error(f"Failed to retrieve data from database: {e}")
        raise


def retrieve_data_from_db_to_list_of_dict(table_name: str, db_name: str = TEKKX_SCALABLE_DB_NAME) -> List[Dict]:
    """
    Retrieve data from a MySQL database and load it into a pandas DataFrame.

    Args:
        table_name (str): The name of the table to retrieve data from.
        db_name (str): The name of the database.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved data.

    Raises:
        Exception: If the data retrieval fails.
    """
    logger, engine = get_engine_for_tekkx_scalable_db(db_name)

    try:
        with engine.begin() as conn:
            df = pd.read_sql_query(
                sql=text(f"SELECT * FROM {table_name}"), con=conn)
            logger.info(f"Data retrieved from {db_name}.{table_name}")
            return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Failed to retrieve data from database: {e}")
        raise
