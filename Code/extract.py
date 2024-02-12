import pandas as pd

def extract_from_csv(file_path):
    '''
    extracts csv data and converts to pandas Dataframe
    args:
        file_path (str): path to the csv file
    
    returns:
        df (DataFrame): pandas dataframe containing the csv data
    ''' 
    dataframe = pd.read_csv(file_path) 
    return dataframe

