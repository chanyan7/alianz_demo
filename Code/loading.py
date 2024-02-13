import pandas as pd
import configparser
import psycopg2
from psycopg2 import sql
from decrypt_pass import decrypt_password
from sqlalchemy import create_engine
from transform import transform_data
from extract import extract_from_csv


def load(cleaned_data: pd.DataFrame, dropped_data: pd.DataFrame):

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

    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=hostname,
        database=database,
        user=username,
        password=encrypted_db_password
    )
    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Define the table name
        table_name = config['Tables']['save_to']
        
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
            

        #state = 1 successful 0 failure    
        state = True
        e='ETL process completed successfully'

    except psycopg2.Error as e:
        print("Error:", e)
        state = False
        conn.rollback()
    finally:

        # Execute the SQL query to get the current time
        cursor.execute("SELECT CURRENT_TIMESTAMP")

        # Fetch the result
        current_time = cursor.fetchone()[0]

        #logs 
        cursor.execute("INSERT INTO sales.logs (state, logs_detail, job_description, creation_date) VALUES (%s, %s, %s, %s)", (state, e, 'sales ETL pipeline',current_time))

        # Commit the transaction
        conn.commit()

        print("ETL process completed successfully.")
        # Close the database connection
        cursor.close()
        conn.close()
    
    
    return 1

#cleaned_data, dropped_data = transform_data(pd.read_csv('C:/Users/david/allianz_demo/mock data/part1_mock_data.csv'),pd.Timestamp('1969-01-01 08:30:00'))
#load(cleaned_data, dropped_data )