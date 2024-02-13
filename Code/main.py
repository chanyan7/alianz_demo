import pandas as pd
import psycopg2
from psycopg2 import sql
import configparser
from extract import extract_from_csv
from transform import transform_data
from loading import load
from decrypt_pass import decrypt_password

def run_pipeline():

    config = configparser.ConfigParser()
    config.read('config.ini')

    hostname = config['database']['host']
    database = config['database']['database']
    schema = config['database']['schema']
    username = config['database']['user']
    port = config['database']['port']
    csv_file_path = config['Paths']['csv_file_path']
    table_name = config['Tables']['save_to']           # Define the table name

    # Get the encrypted password
    encrypted_db_password = decrypt_password(config['database']['password'])

    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=hostname,
        database=database,
        user=username,
        password=encrypted_db_password
    )
    # Create a cursor object
    cursor = conn.cursor()

    try:

        #last_timestamp 
        # Execute the SQL query to get the current time
        cursor.execute(f"SELECT max(sale_date) from {schema}.{table_name}")

        # Fetch the result
        last_timestamp  = cursor.fetchone()[0]
        #last_timestamp = '1969-01-01 08:30:00'
 
        # extract
        df = extract_from_csv(csv_file_path)

        # transform
        cleaned_data, dropped_data = transform_data(df, pd.Timestamp(last_timestamp ))

        # load
        data_loaded = load(cleaned_data, dropped_data, conn, cursor)

        state = True
        exp = f'ETL process completed successfully and {data_loaded} registers are loaded'

    except psycopg2.Error as e:
        exp = str(e)
        print("Error:", e)
        state = False
        conn.rollback()
        
    except Exception as e:

        exp = str(e)
        print("Error:", exp)
        state = False

    finally:

        # Execute the SQL query to get the current time
        cursor.execute("SELECT CURRENT_TIMESTAMP")

        # Fetch the result
        current_time = cursor.fetchone()[0]

        #logs 
        cursor.execute("INSERT INTO sales.logs (state, logs_detail, job_description, creation_date) VALUES (%s, %s, %s, %s)", (state, exp, 'sales ETL pipeline1',current_time))

        # Commit the transaction
        conn.commit()

        print("ETL process completed successfully.")
        # Close the database connection
        cursor.close()
        conn.close()

    return


if __name__ == "__main__":

    
    # run pipeline
    run_pipeline()

