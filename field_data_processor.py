import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging

class FieldDataProcessor:
    """
    A class for processing field data, including ingesting data from an SQL database,
    renaming columns, applying corrections, and mapping weather station data.

    Attributes:
    - db_path (str): The path to the SQL database.
    - sql_query (str): The SQL query to retrieve data from multiple tables.
    - columns_to_rename (dict): A dictionary specifying columns to be renamed.
    - values_to_rename (dict): A dictionary specifying values to be renamed.
    - weather_map_data (str): The URL for weather station mapping data.
    - logger (logging.Logger): The logger for recording log messages.
    - df (pd.DataFrame): The DataFrame to store the processed data.
    - engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for database interaction.

    Methods:
    - initialize_logging(logging_level): Set up logging for the instance.
    - ingest_sql_data(): Ingest data from the SQL database.
    - rename_columns(): Rename specified columns in the DataFrame.
    - apply_corrections(column_name='Crop_type', abs_column='Elevation'): Apply corrections to DataFrame columns.
    - weather_station_mapping(): Map weather station data to the DataFrame.
    - process(): Perform the complete data processing pipeline.
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize a FieldDataProcessor instance.

        Args:
            - config_params (dict): A dictionary containing configuration parameters for the class.
              It should include the following keys:
            - 'db_path' (str): The path to the SQL database.
            - 'sql_query' (str): The SQL query to retrieve data from multiple tables.
            - 'columns_to_rename' (dict): A dictionary specifying columns to be renamed.
            - 'values_to_rename' (dict): A dictionary specifying values to be renamed.
            - 'weather_mapping_csv' (str): The URL for weather station mapping data.
            - logging_level (str): The desired logging level (default is "INFO").
        """
        
        self.db_path = config_params['db_path']
        self.sql_query = config_params["sql_query"]
        self.columns_to_rename = config_params["columns_to_rename"]
        self.values_to_rename = config_params["values_to_rename"]
        self.weather_map_data = config_params["weather_mapping_csv"]

        
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None
        
    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Use self.logger.info(), self.logger.debug(), etc.


    def ingest_sql_data(self):
        """
        Ingest data from the SQL database and store it in the DataFrame.

        Returns:
        - pd.DataFrame: The DataFrame containing the ingested data.

        """
        # Check if the engine is not None (not created yet)
        if self.engine is None:
            self.engine = create_db_engine(self.db_path)
            self.logger.info("Database engine created successfully.")
            
        try:
            # Query the data from the database using the created engine
            self.df = query_data(self.engine, self.sql_query)
            self.logger.info("Successfully loaded data.")
            
        except Exception as e:
            # Log an error message if theres's an issue with querying the data
            self.logger.error(f"Failed to load data. Error: {e}")
            
            raise e
            
        return self.df
    
    
    def rename_columns(self):
        """
        Rename specified columns in the DataFrame.

        """
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]       

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"
            
        
        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        
        self.logger.info(f"Swapped columns: {column1} with {column2}")
        
    
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Apply corrections to specified columns in the DataFrame.

        Args:
        - column_name (str): The name of the column to apply corrections to (default is 'Crop_type').
        - abs_column (str): The name of the column to take the absolute value of (default is 'Elevation').

        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))
        self.df[column_name] = self.df[column_name].str.strip()

    def weather_station_mapping(self):
        """
        Map weather station data to the DataFrame.

        Returns:
        - pd.DataFrame: The DataFrame with weather station data mapped.

        """
        self.df = self.df.merge(read_from_web_CSV(self.weather_map_data))
        return self.df

    def process(self):
        """
        Perform the complete data processing pipeline.

        This method sequentially calls the ingest, rename, correction, and mapping methods.
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        self.df = self.df.drop(columns="Unnamed: 0")

        self.logger.info("Data processing completed successfully.")