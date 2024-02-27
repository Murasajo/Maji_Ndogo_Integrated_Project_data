"""
data_ingestion Module

This module provides functions for ingesting data into the Maji Ndogo farm survey database.
It includes functions for creating a database engine, querying data from the database, and reading CSV files from web URLs.

Functions:
    - create_db_engine(db_path): Creates a SQLAlchemy engine for the specified database path.
    - query_data(engine, sql_query): Executes a SQL query on the given database engine and returns the result as a Dataframe.
    - read_from_web_CSV(URL): Reads a CSV file form a web URL and returns the data as DataFrame.

Module-level Variables:
    -logger: A logger named 'data_ingestion' to handle module-specific logs.
    -db_path: The path to the Maji Ndogo farm survey SQLite database.
    -sql_query: The SQL query to fetch data from various tables in the database.
    -weather_data_URL: URL for the weather station data CSV file.
    -weather_mapping_data_URL: URL for the weather data field mapping CSV file.
    
Example:
    ```
    from data_ingestion import create_db_engine, query_data, read_from_web_CSV
    
    # Create a database engine
    engine =  create_db_engine('sqlite:///Maji_Ndogo_farm_survey_small.db')
    
    # Query data from the database 
    data_frame = query_data(engine, 'SELECT * FROM geographic_features LEFT JOIN ...')
    
    # Read CSV from a web URL
    web_data = read_from_web_CSV('https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv')
    ```
"""

from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def create_db_engine(db_path):
    """
    Creates a SQLAlchemy engine for the specified database path.
    
    Args:
        db_path (str): The path to the SQLite database.
        
    Returns:
        engine(sqlalchemy.engine.Engine): The SQLAlchemy engine object.
        
    Raises:
        ImportError: If SQLAlchemy is not installed.
        Exception: If there's an issue creating the database engine.
        
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    Executes a SQL query on the given database engine and returns the result as a DataFrame.
    
    Args:
        engine(sqlalchemy.engine.Engine): The SQLAlchemy engine object.
        sql_query(str): The SQL query to execute.
        
    Returns:
        df (Dataframe): The result of the SQL query as a DataFrame.
        
    Raises:
        ValueError: If the query result is an empty DataFrame.
        Exception: For any other query-related errors.
        
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    Reads a CSV file from a web URL and returns the data as a Dataframe.
    
    Args:
        URL (str): The URL of the CSV file.
        
    Returns:
        df (pandas.DataFrame): The data from the CSV file as a DataFrame.
        
    Raises:
        pd.errors.EmptyDataError: If the URL does not point to a valid CSV file.
        Exception: For any other errors while reading the CSV from the web.
        
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e