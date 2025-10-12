import pandas as pd
from tabulate import tabulate 
import warnings 
import psycopg2  
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')
import urllib.request, json 

# Connect the postgre
engine = create_engine("postgresql://Project_wrengling:Dataengineer@localhost:5432/project_wrengling")
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
    
# print(requirements_table)
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


table_name_dict = {
    'actor':'actor_df',
    'store':'store_df',
    'address':'address_df',
    'category':'category_df',
    'customer':'customer_df',
    'film_actor':'film_actor_df',
    'film_category':'film_category_df',
    'inventory':'inventory_df',
    'language':'language_df',
    'rental':'rental_df',
    'staff':'staff_df',
    'payment':'payment_df',
    'film':'film_df',
    'city' : city_df, 
    'country' : country_df
}

