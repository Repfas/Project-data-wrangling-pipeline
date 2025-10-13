import pandas as pd
from tabulate import tabulate 
import warnings 
import psycopg2  
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')
import urllib.request, json 

# Connect the postgre database
dbname = "dvdrental_clean"  # Sesuai dengan docker-compose
user = "root"               # Sesuai dengan docker-compose  
password = "qwerty123"
host = "localhost"
port = "5433"              # Port yang di-expose Docker

engine_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(engine_str)

connection = engine.connect()

# engine_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
# engine = create_engine(engine_str)
# Source 1 
city_raw = "https://raw.githubusercontent.com/rahilpacmann/case-data-wrangling-api/main/city.csv"
# Source 2
country_raw = "https://raw.githubusercontent.com/rahilpacmann/case-data-wrangling-api/main/country.csv"
# Source 3
requirements_table_url = 'https://rahilpacmann.github.io/case-data-wrangling-api/requirements_table.json'
# load the json
with urllib.request.urlopen(requirements_table_url) as url:
    requirements_table = json.load(url)

print(requirements_table)
# get data city_df
city_df = pd.read_csv(city_raw)
# print(city_df.head())

# get country_df
country_df = pd.read_csv(country_raw)
# print(country_df)

# create function to get table from database
def get_table(table_name, engine):
    try:
        query = f'SELECT * FROM {table_name}'
        dataframe = pd.read_sql(query, engine)
        return dataframe
    except Exception as e:
        print(f'Failed to get a data from {table_name}: {e}')
        return pd.DataFrame()
    #create dataframe each table 
actor_df = get_table('actor', engine)
store_df = get_table('store', engine)
address_df = get_table('address', engine)
category_df = get_table('category', engine)
customer_df = get_table('customer', engine)
film_actor_df = get_table('film_actor', engine)
film_category_df = get_table('film_category', engine)
inventory_df = get_table('inventory',engine)
language_df = get_table('language',engine)
rental_df = get_table('rental',engine)
staff_df = get_table('staff',engine)
payment_df = get_table('payment',engine)
film_df = get_table('film',engine)

# summarize in one dict
table_name_dict = {
    'actor':actor_df,
    'store':store_df,
    'address':address_df,
    'category':category_df,
    'customer':customer_df,
    'film_actor':film_actor_df,
    'film_category':film_category_df,
    'inventory':inventory_df,
    'language':language_df,
    'rental':rental_df,
    'staff':staff_df,
    'payment':payment_df,
    'film':film_df,
    'city' : city_df, 
    'country' : country_df
}
# DATA VALIDATION 
# create function check requirement
# 

def check_table_requirement(actual_table,requirement_table):
    table_actual = list(actual_table.keys())
    table_requirement = list(requirement_table.keys())
    table_checking = []
    for table_name in table_requirement:
        if table_name in table_actual:
            table_checking.append([table_name,"Yes"])
        else:
            table_checking.append([table_checking,"No"])

    headers = ['table_name',"Exist?"]
    table_tabulate = tabulate(tabular_data=table_checking,headers=headers,tablefmt="heavy_grid")
    print(table_tabulate)

check_table_requirement(actual_table=table_name_dict,requirement_table=requirements_table)

# CHECK DATA SHAPE 
# create a function check shape

def check_shape(actual_table):
    # create empty list
    table_shape = []
    for table_name in actual_table:
        number_shape_row = actual_table[table_name].shape[0]
        number_shape_col = actual_table[table_name].shape[1]
        table_shape.append([table_name,number_shape_col,number_shape_row])

    headers = ['table_name','number_column','number_row']  
    table_tabulate = tabulate(tabular_data=table_shape, headers=headers,tablefmt='heavy_grid')
    print(table_tabulate)
check_shape(actual_table=table_name_dict)  

# print(requirements_table)

# Checking a columns 
def checking_column(actual_table,requirement_table):
    table_col_req = []
    for table_name in requirement_table:
        table_col_req.append(table_name)
        col_act = list(actual_table[table_name])
        table_both = set(table_col_req + table_col_act)
        for 




checking_column(actual_table=table_name_dict,requirement_table=requirements_table)







