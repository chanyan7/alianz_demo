import pandas as pd
import datetime


def transform_data(df: pd.DataFrame, last_timestamp):
    """
    Transform, cleaning data to be loaded in SQL Database
    args:
        df : data from CSV stored in a dataframe
        last_timestamp: last ejecution of ETL process
    
    returns:
        df (DataFrame): pandas dataframe to be loaded in SQL database
    """
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # loading new records
    df = df[df['timestamp'] > last_timestamp]
    # Check the duplication in primary key
    
    #drop duplicates
    dropped_rows_df =  df[df.duplicated(keep=False)]
    df.drop_duplicates()
    warning_msg = 'Tranform process: there are duplicated values'

    #drop nul values
    null_values_df = df[df.isnull().any(axis=1)]
    df.dropna()
    warning_msg = 'Transform process: there are null values'

    # check for negative values
    negative_quant = df[df['quantity'] <= 0]
    df = df[df['quantity'] > 0]
    warning_msg = 'Transform process: there are negative quantity'

    print(dropped_rows_df)
    
    return 1

transform_data(pd.read_csv('C:/Users/david/allianz_demo/mock data/part1_mock_data.csv'),pd.Timestamp('1969-01-01 08:30:00'))


    
    
