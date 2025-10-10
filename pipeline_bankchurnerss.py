import pandas as pd 

def cleaning_data(data):
    def columns_requirements():
        columns = {'CLIENTNUM' : int,
                'Attrition_Flag' : object,
                'Customer_Age' : int,
                'Gender' : object,
                'Dependent_count' : int,
                'Education_Level' : object,
                'Marital_Status' : object,
                'Income_Category_Min' : int,
                'Income_Category_Max' : int,
                'Card_Category' : object,
                'Months_on_book' : int,
                'Total_Relationship_Count' : int,
                'Months_Inactive_12_mon' : int,
                'Contacts_Count_12_mon' : int,
                'Credit_Limit' : float,
                'Total_Revolving_Bal' : int,
                'Avg_Open_To_Buy' : float,
                'Total_Amt_Chng_Q4_Q1' : float,
                'Total_Trans_Amt' : int,
                'Total_Trans_Ct' : int,
                'Total_Ct_Chng_Q4_Q1' : float,
                'Avg_Utilization_Ratio' : float}
        return columns
    
    def values_requirements():
        values = {
            'Attrition_Flag' : ['Existing Customer', 'Attrited Customer'],
            'Gender' : ['M', 'F'],
            'Education_Level' : ['High School', 'Graduate', 'Uneducated', 'Unknown', 'College', 'Post-Graduate', 'Doctorate'],
            'Marital_Status' : ['Married', 'Single', 'Divorced'],
            'Card_Category' : ['Blue', 'Gold', 'Silver', 'Platinum']
        }
        return values


    print("📊 " + "\033[1;36m" + "ASSET PORTFOLIO ASSESSMENT" + "\033[0m" + " 📊")
    # get the actual coloumn and row 
    number_of_row = data.shape[0]
    number_of_column = data.shape[1]
    print(f'Number of row: {number_of_row}')
    print(f'Number of column: {number_of_column}')
    print('\n\n')
    print('-'*100)
    print("📋 " + "\033[1;32m" + "ACCOUNT LEDGER INVENTORY" + "\033[0m" + " 📋")
    # get set requirement and the actual data
    req_columns = set(columns_requirements().keys())
    actual_columns = set(data.columns)
    
    if actual_columns == req_columns:
        print("Column already match with requirement")
    else:
        print('The column must be delete:')
        for actual_col in actual_columns:
            if actual_col not in req_columns:
                print(actual_col)
        print('The column must be exist in your data:')
        for req_col in req_columns:
            if req_col not in actual_columns:
                print(req_col)
    print('\n\n')
    print('-'*100)
    print("💳 " + "\033[1;33m" + "TRANSACTION TYPE VERIFICATION" + "\033[0m" + " 💳")
    actual_columns = list(actual_columns)
    req_data = columns_requirements()
    not_match = []
    for req_col,type_col in req_data.items():
        if req_col in actual_columns:
            if data[req_col].dtypes != type_col:
                print(f'The column {req_col} has different type data')
                not_match.append(req_col)
        else:
            print(f'The coloumn {req_col} are not exist!')
    for col in not_match:
        print(f'the coloumn {col} is require type data {req_data[col]}')
    print('\n\n')
    print('-'*100)
    print("🔍 " + "\033[1;31m" + "FRAUD DETECTION SCAN" + "\033[0m" + " 🔍")
    # find a missing data 
    actual_columns = actual_columns
    missing_value_col = {}
    for act_col in actual_columns:
        persentage_null =data[act_col].isna().sum()/len(data[act_col])*100
        if persentage_null > 0 :
            missing_value_col[act_col] = persentage_null
    if missing_value_col:
        for col,persentage in missing_value_col.items():
                print(f'the coloumn {col} has {round(persentage,2)}% null data')
    else:
        print('No missing data in this dataset')

    # check duplicate data
    duplicate_data = data.duplicated(keep = False).sum()
    if duplicate_data:
        print(f'There are {duplicate_data} duplicates data')
    else:
        print('There is no duplicate data')
    print('\n\n')
    print('-'*100)   
    print("🔍 " + "\033[1;31m" + "FRAUD DETECTION SCAN" + "\033[0m" + " 🔍")
    required_values = values_requirements()
    missing_required_dict ={}
    unexpected_actual_dict = {}
   
    
    for col,req_values in required_values.items():
        actual_value = list(data[col].unique())
        missing_required_values = []
        unexpected_actual_values = []
        # for missing in required value
        for val in req_values:
            if val not in actual_value:
                missing_required_values.append(val)
        missing_required_dict[col] = missing_required_values

        # for unexpected values 
        for val in actual_value:
            if val not in req_values:
                unexpected_actual_values.append(val)
        unexpected_actual_dict[col] = unexpected_actual_values

    print(unexpected_actual_dict)
    print(missing_required_dict)
        
        

bank_churners = pd.read_csv('live class/BankChurners.csv')
cleaning_data(data=bank_churners)