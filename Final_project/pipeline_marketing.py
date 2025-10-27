import psycopg2 
import pandas as pd
from sqlalchemy import create_engine
from tabulate import tabulate


# connect to db on docker
dbname = "etl_db"  
user = "postgres"               
password = "password123"
host = "localhost"
port = "5432"
engine_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
query = 'SELECT * FROM amazon_sales_data'
sales_df = pd.read_sql(query,engine_str)

marketing_df = pd.read_csv('ElectronicsProductsPricingData.csv')


def check_shape(table_name, actual_table):
    shape_col = []
    num_col = actual_table.shape[1]
    num_row = actual_table.shape[0]
    shape_col.append([num_col,num_row])
    headers = ['number_col','number_row']
    table = tabulate(tabular_data=shape_col,headers=headers)
    print(table_name)
    print(table)


check_shape('sales_df',sales_df)
check_shape('marketing_df',marketing_df)


def check_table_name(table_name, actual_data):
    cols_name = []
    for col_name in actual_data:
        cols_name.append([col_name,actual_data[col_name].dtypes])
    
    headers = ['table_name','data_type']
    table = tabulate(tabular_data= cols_name, headers=headers)
    print(table_name)
    print(table)
check_table_name('sales_df',sales_df)
check_table_name('marketing_df',marketing_df)

def missing_value(table_name, actual_data):
    missing_data = []
    for col_name in actual_data:
        missing_value = actual_data[col_name].isnull().sum()/len(actual_data[col_name])
        if missing_value*100 > 0:
            missing_data.append([col_name,missing_value*100])
    
    headers = ['column_name','percentage_value(100)']
    table = tabulate(tabular_data=missing_data,headers= headers)
    print(table_name)
    print(table)


missing_value('sales_df',sales_df)
missing_value('marketing_df',marketing_df)



def check_duplicate(table_name, actual_data):
    duplicate_row = actual_data.duplicated(keep = False).sum()
    number_row = len(actual_data)
    percentage_duplicate = duplicate_row/number_row * 100
    data_for_table = [[duplicate_row, number_row, f"{percentage_duplicate:.2f}%"]]
    headers = ['number_of_duplicate','number_row','percentage_duplicate']
    table = tabulate(tabular_data=data_for_table,headers=headers)
    print(table)


check_duplicate('sales_df',sales_df)
check_duplicate('marketing_df',marketing_df)

# def samples_data(table_name, actual_data):
#     print(table_name)
#     for col_name in actual_data:
#         samples = actual_data[col_name].dropna().head(3).tolist()
#         print(col_name)
#         print(samples)

    


# samples_data('sales_df',sales_df)
# samples_data('marketing_df',marketing_df)