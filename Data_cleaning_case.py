import pandas as pd
import tabulate



def columns_requirements():
    columns = {'order_id' : int,
               'product' : object,
               'quantity_orderd' : int,
               'price_each' : float,
               'order_date' : 'datetime64[ns]',
               'street' : object,
               'city' : object,
               'zip_code' : object
               }
    return columns

sales_dataF = pd.read_csv("live class/sales_data.csv")

print("╔══════════════════════════════╗")
print("║   🚀 DATA SHAPE ANALYSIS     ║")
print("╚══════════════════════════════╝")

column_shape = sales_dataF.shape[1]
row_shape = sales_dataF.shape[0]

print(f'Number of columns {column_shape}')
print(f'Number of row {row_shape}')

print("╔══════════════════════════════╗")
print("║   🚀 Column check            ║")
print("╚══════════════════════════════╝")
# get a set of requirement and actual one
requirement_need = set(columns_requirements().keys())
actual_data = set(sales_dataF.columns)

if requirement_need == actual_data:
    print('column match in your database')
else:
    for req_data in requirement_need:
        if req_data not in actual_data:
            print(f'The data need is {req_data}')
    for data in actual_data:
        if data not in requirement_need:
            print(f'The data column should be removed {data}')

# check the data type 
req_data = columns_requirements()
actual_data = list(actual_data)
print(actual_data)
not_match_columns = []

for req_column,req_type_data in req_data.items():
    if req_column in actual_data:
        if req_type_data != sales_dataF[req_column].dtypes:
            not_match_columns.append(req_column)
    else:
        print(f'{req_column} is skipped for checking because this column doesnt exist in data')

if not_match_columns:
    print('There are some columns not matched')
    print('The columns not match is:')
    for column in not_match_columns:
        print(f'{column} must be {req_data[column]}')
else:
    print("All checked column type are match with requirement")

    