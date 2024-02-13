import pandas as pd
import psycopg2
from psycopg2 import sql
import configparser
from extract import extract_from_csv
from transform import transform_data
from loading import load
from decrypt_pass import decrypt_password

def run_pipeline(csv_file_path:str):

    #last_timestamp 

    # extract
    df = extract_from_csv(csv_file_path)

    # transform
    cleaned_data, dropped_data = transform_data(df, pd.Timestamp('1969-01-01 08:30:00'))

    # load
    load(cleaned_data, dropped_data)

    return


if __name__ == "__main__":

    # Read the encrypted passwords from config.ini
    config = configparser.ConfigParser()
    config.read('config.ini')


    file_path = config['Paths']['csv_file_path']
    # run pipeline
    run_pipeline(file_path)

