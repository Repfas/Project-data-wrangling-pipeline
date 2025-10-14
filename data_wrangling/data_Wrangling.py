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
    for table_name in requirement_table:
        act_cols = list(actual_table[table_name].columns)
        req_tables = requirement_table[table_name]
        req_cols = []
        for table in req_tables:
            req_cols.append(table['column_name'])
        existance = []
        both_cols = set(act_cols + req_cols)
        for col in both_cols:
            if col in act_cols:
                mark_act = 'YES'  
            else:
                mark_act = 'NO'
            if col in req_cols:
                mark_req = 'YES'
            else:
                mark_req = 'NO'
            existance.append([col,mark_act,mark_req])
        if set(act_cols) == set(req_cols):
            pass 
        else:
            headers = ['column_name',"existance_actual_table","existance_requirement_table"]
            table_result = tabulate(tabular_data=existance,headers=headers,tablefmt='heavy_grid')
            print(table_name)
            print('\n')
            print(table_result)
            print('-'*70)

checking_column(actual_table=table_name_dict,requirement_table=requirements_table)
# check data_types 
# create function check data types 

def check_data_type(actual_data,requirement_data):
    missmatch_data = []
    for table_name,cols_req in requirement_data.items():
        col_req_types = {}
        for col_req in cols_req:
            col_req_name = col_req['column_name'] 
            col_req_type = col_req['data_type']
            col_req_types[col_req_name] = col_req_type
        
        counter = 0 
        col_act_types = {}
        while  counter < len(actual_data[table_name].columns) : 
            col_act_types[list(actual_data[table_name].columns)[counter]] = list(actual_data[table_name].dtypes)[counter]
            counter +=1
        
        for req_col_name,req_type in col_req_types.items():
            if req_col_name not in col_act_types:
                missmatch_data.append([table_name,req_col_name,req_type,'Not Found',f'Not Match: the column {req_col_name} not found'])
            else:
                if col_req_types[req_col_name] == col_act_types[req_col_name]:
                    pass 
                else:
                    missmatch_data.append([table_name,req_col_name,req_type,col_act_types[req_col_name],'Not Match'])
        
        for col_act_name in col_act_types:
            if col_act_name not in col_req_types:
                missmatch_data.append([table_name,col_act_name,'Not Found',col_act_types[col_act_name],f'Not Match: the coloumn {col_act_name} are not required'])

    if missmatch_data:
        headers = ['table_name','column_name','Requirement_type','actual_type','Status']
        table_data = tabulate(missmatch_data,headers=headers,tablefmt='heavy_grid')
        print(table_data)
    else:
        print('✅ All data types match requirements!')
check_data_type(actual_data=table_name_dict,requirement_data=requirements_table)


# check missing value
def missing_value(actual_table):
    null_data = []
    for table_name in actual_table:
        for col_name in list(actual_table[table_name]):
            null_value = actual_table[table_name][col_name].isnull().sum()
            if null_value >0 :
                null_data.append([table_name,col_name,null_value,round(null_value/len(actual_table[table_name])*100,2)])
            

    if null_data:
        header = ['table_name','column_name','missing_value_count','missing_value_persentation(%)']
        table = tabulate(null_data,headers=header,tablefmt='heavy_grid')
        print(table)
    else:
        print('No missing Value')
missing_value(table_name_dict)

# Check duplicate_data 
def duplicate_data(actual_table):
    dups_data = []
    for table_name,df in actual_table.items():
        try:
            duplicate_row = df[df.duplicated(keep= False)]
            if not duplicate_row.empty:
                dups_data.append([table_name,len(duplicate_row)])
                
        except:
            pass

    headers = ['table_name','number_of_duplicate']
    table = tabulate(dups_data,headers=headers,tablefmt='heavy_grid')
    print(table)

duplicate_data(actual_table=table_name_dict)