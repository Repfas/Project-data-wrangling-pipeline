import pandas as pd
import tabulate


def data_validation(data):
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

    

    print("\033[1m>>>>> STEP 1 : CHECK DATA SHAPE \033[0m")

    column_shape = data.shape[1]
    row_shape = data.shape[0]
    
    print(f'Number of columns {column_shape}')
    print(f'Number of row {row_shape}')
    print("\n\n")
    print('-'*100)  
    print("\033[1m>>>>> STEP 2 : CHECK COLUMNS \033[0m")
    # get a set of requirement and actual one
    requirement_need = set(columns_requirements().keys())
    actual_data = set(data.columns)
    col_needed = []
    if requirement_need == actual_data:
        print('column match in your database')
    else:
        for req_col in requirement_need:
            if req_col not in actual_data:
                print(f'The data need is {req_col}')
                col_needed.append(req_col)
        for col in actual_data:
            if col not in requirement_need:
                print(f'The data column should be removed {col}')
    print("\n\n")
    print('-'*100)
    print("\033[1m>>>>> STEP 3 : CHECK DATA TYPES \033[0m")
    # check the data type 
    req_data = columns_requirements()
    actual_data = list(actual_data)
    print(actual_data)
    not_match_columns = []

    for req_column,req_type_data in req_data.items():
        if req_column in actual_data:
            if req_type_data != data[req_column].dtypes:
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
    print("\n\n")
    print('-'*100)
    print("\033[1m>>>>> STEP 4 : CHECK MISSING VALUES \033[0m")
    actual_data = list(actual_data)
    missing_value_persentages = {}
    for col in actual_data:
        persentage_miss = data[col].isna().sum()/ len(data)*100
        if persentage_miss >0:
            missing_value_persentages[col] = persentage_miss
    print('the missing values columns is')
    for miss_col,persentage in missing_value_persentages.items():
        print(''*50,f'-{miss_col}:{round(persentage,2)}%')
    print("\n\n")
    print('-'*100)
    print("\033[1m>>>>> STEP 5 : CHECK DUPLICATES DATA \033[0m")
    # find the number of duplicate data
    count_duplicate = data.duplicated(keep= False).sum()
    if count_duplicate:
        print(f'There are {count_duplicate} duplicates data')
    else:
        print('There is no duplicates data')
    # need to kill the Nan first
    
    # extract data from column needed
    # add the col need to the dataframe 

    return data





sales_dataF = pd.read_csv("live class/sales_data.csv")
data = sales_dataF
data_validation(data)
data['street'] = data['purchase_address'].str.extract(r'(\d+\s[A-Za-z0-9\s]+),')
data['city'] = data['purchase_address'].str.extract(r',\s([A-Za-z\s]+),')
data['zip_code'] = data['purchase_address'].str.extract(r'([A-Z]{2}\s\d+)')
data.drop(columns = 'purchase_address', inplace = True)
data.dropna(inplace= True)
data.drop_duplicates(keep='first',inplace=True)
data['order_id'] =data['order_id'].astype(dtype='int')
data['quantity_orderd']= data['quantity_orderd'].astype(dtype='int')
data['order_date']= data['order_date'].astype(dtype='datetime64[ns]')
data_validation(data)

