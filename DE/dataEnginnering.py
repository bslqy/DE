
# coding: utf-8

# In[1]:

def clean_and_hash_account_id_column(x):
    # convert to str
    str_x = str(x)
    # use regex to match integer value
    if re.match(r'.*?\.0',str_x):
        return hash(str_x)
    return None

def clean_active_indicator_column(x):
    try:
        return int(x)
    except:
        return None
    

def clean_account_type_column(x):
    if x in ['B2B','B2C']:
        return x
    return None


def clean_account_status_column(x):
    if x in ['DISP','PAID','REVR']:
        return x
    return None


def clean_FIBRE_column(x):
    if re.match(r'^[a-zA-z].*',x):
        return x
    return None


def clean_Property_TYPE_column(x):
    if x in ['RESI','BSME','BEST']:
        return x
    return None


# In[2]:

from datetime import datetime as dt
def clean_dataset(df):
    df['Account_ID '] = df['Account_ID '].apply(lambda x: clean_and_hash_account_id_column(x))
    df['Active Indicator '] = df['Active Indicator '].apply(lambda x: clean_active_indicator_column(x))
    df['Account Type '] = df['Account Type '].apply(lambda x: clean_account_type_column(x))
    df['Account status '] = df['Account status '].apply(lambda x: clean_account_status_column(x))
    df['FIBRE '] = df['FIBRE '].apply(lambda x: clean_FIBRE_column(x))
    df['Property TYPE '] = df['Property TYPE '].apply(lambda x: clean_Property_TYPE_column(x))
    # calcualte the response time
    df['Response_Time'] = df['Implemented Date '].apply(lambda x: dt.strptime(x,"%d/%m/%Y %H:%M")) - df['Request Date '].apply(lambda x: dt.strptime(x,"%d/%m/%Y %H:%M"))
    return df


# In[3]:

def batch_write_json(df,out_dir='./'):
    import os
    length = len(df)
    number_of_file = 0
    last_file_number = 0
    last_carry = 0
    # divisible number of line
    if (length % 1000) == 0:
        number_of_file = length // 1000
        last_file_number = number_of_file
    else:
        number_of_file = length // 1000 + 1
        last_file_number = number_of_file
        last_carry = length % 1000
        
    print(f'number_of_file:{number_of_file},last_file_number:{last_file_number},last_carry:{last_carry}')
            
    # Starting file name
    current_index = 1
    # iterative through every file        
    while (current_index <= number_of_file):
        print(f"processing file: {current_index}")
        os.path.join(out_dir,f'{current_index}.json')
        with open(os.path.join(out_dir,f'{current_index}.json'), 'a') as outfile:
            if current_index == last_file_number:
                print("Last file!")
                if last_carry != 0:
                    json.dump(df[1000 * (current_index - 1) :1000 * (current_index - 1) + last_carry ].to_json(orient='records')[1:-1],outfile)
                elif last_carry == 0:
                    json.dump(df[1000 * (current_index - 1) : 1000 * current_index].to_json(orient='records')[1:-1],outfile)
            json.dump(df[1000 * (current_index - 1) : 1000 * current_index].to_json(orient='records')[1:-1],outfile)
        current_index = current_index + 1   
    
    print(f"Successfully wrote Json file to {outfile}")
    return True



# In[4]:

def main(file_path):
    try:
        data = pd.read_csv(file_path)
    except:
        raise FileNotFoundError(f'{file_path} does not exist!')
    
    # insecpt the data
    data.head()
    
    # Filter out the two Unnamed columns
    output_columns = ['Account_ID ',
     'CODE ',
     'Implemented Date ',
     'Active Indicator ',
     'Account Type ',
     'Service ',
     'BU',
     'Request Date ',
     'Account status ',
     'Status Code ',
     '$ Amount ',
     'Version ',
     'Agent ID ',
     'FIBRE ',
     'last Updated Date ',
     'Property TYPE ',
     'Post Code ']
    
    # Made a wokring copy
    data_copy = data.copy()
    
    # Filter all out all the questonable data (i.e. value == NULL)
    clean_df = clean_dataset(data_copy)
    clean_df = data_copy[((~data_copy['Account_ID '].isnull()) & (~data_copy['Account Type '].isnull()) & (~data_copy['Active Indicator '].isnull()) & (~data_copy['Account status '].isnull()) & (~data_copy['FIBRE '].isnull()) & (~data_copy['Property TYPE '].isnull()) )]
   
    # List post code based on fastest response
    post_code_by_response_time = clean_df.groupby(['Post Code '])['Response_Time'].min()
    
    # TOP agent based on postcode and amount
    top_agent_by_postcode_df = clean_df.loc[clean_df.groupby(['Post Code '])["$ Amount "].idxmax()][['Post Code ','Agent ID ','$ Amount ']]  
    
    # select output columns
    df_output = clean_df[output_columns]
    
    # write to Jsom for every 1000 lines
    if not batch_write_json(df_output):
        raise RuntimeError("JSON files are not written!")


# In[5]:

if __name__ == '__main__':
    import pandas as pd
    import numpy as np
    import re
    import json
    import os
    # try:
    #     file_full_path = str(sys.argv[1])
    # except Exception as e:
    #     raise ValueError(f"Usage : python3 dataEnginnering.py [csv_full_path] ")
    main('./Transaction.csv')
    

