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
port = "5433"              

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
    print("╔══════════════════════════════╗")
    print("║   🔍 TABLE EXISTENCE CHECK   ║")
    print("╚══════════════════════════════╝")
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
    print("╔══════════════════════════════╗")
    print("║      📊 DATA SHAPE CHECK     ║")
    print("╚══════════════════════════════╝")
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
# list all table and column that exist in the actual data
def list_table_column(actual_data):
    print("╔══════════════════════════════════════════╗")
    print("║           📋 TABLE & COLUMN LIST         ║")
    print("╚══════════════════════════════════════════╝")
    list_table_and_col = []
    for table_name,df in actual_data.items():
        for column_name in df:
            list_table_and_col.append([table_name,column_name])

    header = ['table_name','column_name']
    table = tabulate(list_table_and_col,headers=header,tablefmt='heavy_grid')
    print(table)

list_table_column(actual_data=table_name_dict)

# Checking a columns 


def checking_column(actual_table,requirement_table):
    print("╔══════════════════════════════╗")
    print("║     🗂️  COLUMN EXISTENCE     ║")
    print("╚══════════════════════════════╝")
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
    print("╔══════════════════════════════╗")
    print("║     📝 DATA TYPE CHECK       ║")
    print("╚══════════════════════════════╝")
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
    print("╔══════════════════════════════╗")
    print("║  🚀 MISSING VALUE ANALYSIS   ║")
    print("╚══════════════════════════════╝")
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
    print("╔══════════════════════════════╗")
    print("║     🔄 DUPLICATE DATA CHECK  ║")
    print("╚══════════════════════════════╝")
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


# DATA TRANSFORM 
# 1. Data cleansing 
# Cleaning the data 
# a. handle the missing column 
# a.1 create country_id in table country 
table_name_dict['country']['country_id'] = [x for x in range (1,110)]



#data country_id in table city is not existance. need to join the data country_id and city 
# to create a country_id

city_join_country = table_name_dict['city'].join(table_name_dict['country'].set_index('country'))
city_join_country= city_join_country.drop(columns='country')
table_name_dict['city'] = city_join_country

# handle missing value --> create function of it
def remove_missing_value(actual_data):
    cleanned_actual_table = {}
    for table_name,df in actual_data.items():
        try:
            cleanned_actual_table[table_name] = df.dropna() 
        except:
            pass
    return cleanned_actual_table

table_name_dict = remove_missing_value(table_name_dict)




def adjust_data_type(actual_data:pd.DataFrame,requirement_data):
    adjust_data_dict = {}
    for table_name, df in actual_data.items():
        if table_name in requirement_data:
            req_table = requirement_data[table_name]
            for detail_req in req_table:
                col_name = detail_req['column_name']
                type_req = detail_req['data_type']
                if col_name in df.columns:
                    df[col_name] = df[col_name].astype(type_req)
                    adjust_data_dict[col_name] = df[col_name]
    
    return adjust_data_dict

adjust_data_type(actual_data=table_name_dict,requirement_data=requirements_table)



def remove_duplicates(actual_data):
    non_duplicate = {}
    for table_name,df in actual_data.items():
        try:
            df.drop_duplicates(keep= 'first',inplace = True)
            non_duplicate[table_name] = df 
        except:
            pass 
    return non_duplicate

remove_duplicates(actual_data=table_name_dict)
print('-'*30,'CHECK AFTER CLEANING DATA','-'*30)
check_table_requirement(actual_table=table_name_dict,requirement_table=requirements_table)
check_shape(actual_table=table_name_dict)  
checking_column(actual_table=table_name_dict,requirement_table=requirements_table)
check_data_type(actual_data=table_name_dict,requirement_data=requirements_table)
missing_value(table_name_dict)
duplicate_data(actual_table=table_name_dict)
list_table_column(actual_data=table_name_dict)

# Manipulation data 
# Objection: to create new table film_list 
# need film_id(fid)(from film),title(from film),description(from film),
# category(category,name),price(film,rental_rate),length(film), rating(film),actor(actor,actor_name)

# merge data
# cat + film_cat

film_list = table_name_dict['category'].merge(table_name_dict['film_category'],how= 'left',left_on='category_id',right_on = 'category_id',suffixes = ("_x1", "_y1"))

# +film
film_list = film_list.merge(table_name_dict['film'],how= 'left',on='film_id')

# + film_actor 
film_list = film_list.merge(table_name_dict['film_actor'],how= 'inner',on = 'film_id')

# + actor 
film_list = film_list.merge(table_name_dict['actor'],how= 'inner',on = 'actor_id',suffixes = ("_x3", "_y3"))

film_list['full_name'] = film_list['first_name'] + ' '+ film_list['last_name']


film_list = film_list.groupby(['film_id', 'title', 'description', 'name', 'rental_rate', 'length', 'rating'])['full_name'].apply(lambda x:', '.join(x))

film_list = film_list.reset_index()
new_name = {
    'film_id':'fid',
    'name':'category',
    'rental_rate':'price',
    'full_name' : 'actors'
}

film_list = film_list.rename(columns=new_name)

def dw_postgres_engine(database_name):
    
    # Koneksi ke database

    user = "root"               # Sesuai dengan docker-compose  
    password = "qwerty123"
    host = "localhost"
    port = "5433"      
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database_name}")

    return engine

engine = dw_postgres_engine(database_name = 'dvdrental_clean')


for table_name, df in table_name_dict.items():

    df.to_sql(table_name, engine, if_exists = 'replace', index = False)


engine.dispose()

engine = dw_postgres_engine(database_name = "dvdrental_analysis")

# Insert data ke table film_list
film_list.to_sql('film_list', engine, if_exists = 'replace', index = False)

# Tutup koneksi ke database
engine.dispose()


