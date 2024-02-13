import pandas as pd
import numpy as np
from datetime import datetime, timedelta

"""
This function is to generate the mock data of sales in a CSV where 
    max_register = size of CSV (editable)
    file_path = the path where csv stores (editable)
"""


#define the size of data
max_register = 20000; 
max_quantity = 100;
start_date = datetime(1990, 2, 9, 9, 10, 0)
last_id = 40000

# Specify the path where you want to save the CSV file
file_path = 'C:/Users/david/allianz_demo/mock data/part1_mock_data.csv'


#list of transactions, generating the data sequencially  
transactions_list = list(range(last_id+1,  last_id + max_register+1)) 
#list of customers, could be duplicated
customers_list = np.random.randint(1, max_register *100 , size = max_register)

#list of products
products_list = np.random.randint(1, max_register *100 , size = max_register)

#list of products
quantity_list = np.random.randint(1, max_quantity , size = max_register)

#list of sale dates
end_date = start_date + timedelta(days=max_register/4)
dates_list = list(pd.date_range(start = start_date, end = end_date, freq="6h"))
dates_list.pop()

#merge all lists into a dataframe
mock_data_csv = pd.DataFrame({'transaction_id': transactions_list, 'customer_id': customers_list, 'product_id': products_list, 'quantity': quantity_list, 'timestamp': dates_list })

# Save the DataFrame to a CSV file
mock_data_csv.to_csv(file_path, index=False) 

print('generation of data successfully')

