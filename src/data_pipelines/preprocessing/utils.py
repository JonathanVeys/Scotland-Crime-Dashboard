import re
import unicodedata
from typing import List, Tuple

import pandas as pd
import pandas as pd



def normalise_text(string: str) -> str:
    if pd.isna(string):
        return ''
    
    # conditions = [' and ', '&', '/', '-']

    # for condition in conditions:
    #     string = re.sub(condition, '', string)


    string = string.lower()
    string = re.sub(' and ', '', string)
    string = re.sub('&', '', string)
    string = re.sub('/', '', string)
    string = re.sub(',', '', string)
    string = re.sub(r'[.]', '', string)
    string = re.sub('-', '', string)
    string = re.sub("'", '', string)
    string = re.sub(r'[()]', '', string)
    string = re.sub(r'[’]', '', string)
    string = re.sub(r"\s+", '', string)
    string = re.sub('agus', '', string)
    string = re.sub('ward$', '', string)

    # Normalize Unicode characters (decompose accents)
    normalized = unicodedata.normalize('NFKD', string)
    string = normalized.encode('ASCII', 'ignore').decode('utf-8')

    return string

def normalise_column_name(col_name:str) -> str:
    col_name = col_name.lower()
    col_name = re.sub('(?<=\w) (?=\w)', '_', col_name)
    col_name = re.sub('\((.*?)\)', '', col_name)
    col_name = re.sub(' ', '', col_name)

    return col_name

def subset_columns(data:pd.DataFrame, columns_to_keep:List[str]):
    '''
    A function that handles taking a subset of columns from a dataframe
    '''
    missing_columns = [column for column in columns_to_keep if column not in data.columns]

    if missing_columns:
        raise ValueError(f'Columns not present in data: {missing_columns}')
    
    data = data[columns_to_keep]
    
    return data

def rename_column_names(data:pd.DataFrame, rename_dict:dict):
    data = data.rename(columns=rename_dict)

    return data

def split_dataset(df:pd.DataFrame, splitting_value:str|int, splitting_col:str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    mask = df[splitting_col] == splitting_value

    sub_df_1 = df[mask]
    sub_df_2 = df[~mask]
    
    return (sub_df_1, sub_df_2)

def date_difference(date1:pd.Timestamp, date2:pd.Timestamp, time):
    diff_year = date1.year - date2.year
    diff_month = date1.month - date2.month
    return diff_year * 12 + diff_month