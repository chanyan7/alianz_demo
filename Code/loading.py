import pandas as pd
import configparser
import psycopg2
from psycopg2 import sql
from decrypt_pass import decrypt_password
from sqlalchemy import create_engine
from transform import transform_data
from extract import extract_from_csv


def load(cleaned_data: pd.DataFrame, dropped_data: pd.DataFrame, conn, cursor):

    """
    Loading data into Databse base on config.ini
    arg:
        cleaned_Data: dataframe
        dropped_Data: dataframe
    
    return:
        loading_state: boolean
    """
    # Read the encrypted passwords from config.ini
    config = configparser.ConfigParser()
    config.read('config.ini')

    hostname = config['database']['host']
    database = config['database']['database']
    schema = config['database']['schema']
    username = config['database']['user']
    port = config['database']['port']
    logs_table = config['database']['logs']

    # Get the encrypted password
    encrypted_db_password = decrypt_password(config['database']['password'])

    # Define the table name
    table_name = config['Tables']['save_to']
    error_table_name = config['Tables']['error_data_save_to']
    
    # Create a SQLAlchemy engine
    baseURL = f'postgresql+psycopg2://{username}:{encrypted_db_password}@{hostname}:{port}/{database}'
    engine = create_engine(baseURL)
    
    # Retrieve existing data from PostgreSQL table
    existing_data_query = f'SELECT * FROM {schema}.{table_name}'
    existing_data = pd.read_sql(existing_data_query, engine)

    # Identify new records to insert
    new_records = cleaned_data[~cleaned_data['transaction_id'].isin(existing_data['transaction_id'])]

    # Insert new records into PostgreSQL table
    if not new_records.empty:
        new_records.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)

    #wrong data no empty
    if not dropped_data.empty:
        dropped_data.to_sql(error_table_name, engine, if_exists='append', index=False, schema=schema)
            
    return len(new_records)

#cleaned_data, dropped_data = transform_data(pd.read_csv('C:/Users/david/allianz_demo/mock data/part1_mock_data.csv'),pd.Timestamp('1969-01-01 08:30:00'))
#load(cleaned_data, dropped_data )