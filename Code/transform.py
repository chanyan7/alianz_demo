import pandas as pd
import datetime


def transform_data(df: pd.DataFrame, last_timestamp):
    """
    Transform, cleaning data to be loaded in SQL Database
    args:
        df : data from CSV stored in a dataframe
        last_timestamp: last ejecution of ETL process
    
    returns:
        df (DataFrame): cleaned data to be loaded in SQL database
        drop_data(DataFrame): dropped data due to data problems 
    """
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # loading new records
    df = df[df['timestamp'] > last_timestamp]
    # Check the duplication in primary key
    
    #drop duplicates
    #dropped_rows_df =  df[df.duplicated(keep=False)]
    df = df.drop_duplicates()
    warning_msg = 'Tranform process: there are duplicated values'

    #drop null values
    null_values_df = df[df.isnull().any(axis=1)]
    df = df.dropna()
    warning_msg = 'Transform process: there are null values'

    # check for negative values
    negative_quant = df[df['quantity'] <= 0]
    df = df[df['quantity'] > 0]
    warning_msg = 'Transform process: there are negative quantity'

    #concatenate the wrong data in 1 dataframe
    dropped_data = pd.concat([null_values_df,negative_quant], axis=1)
    #print(df)
    
    #convertion of data type 
    df['transaction_id'] = df['transaction_id'].astype('int')
    df['customer_id'] = df['customer_id'].astype('int')
    df['product_id'] = df['product_id'].astype('int')
    df['quantity'] = df['quantity'].astype('int')
    df['timestamp'] = df['timestamp'].dt.date

    #rename columne
    df= df.rename(columns={'timestamp': 'sale_date'})

    return df, dropped_data

#transform_data(pd.read_csv('C:/Users/david/allianz_demo/mock data/part1_mock_data.csv'),pd.Timestamp('1969-01-01 08:30:00'))


    
    
