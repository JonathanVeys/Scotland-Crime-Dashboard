import re
import unicodedata
from typing import List, Tuple, Dict, Callable
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np




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

def date_difference(date1:pd.Timestamp, date2:pd.Timestamp):
    diff_year = date1.year - date2.year
    diff_month = date1.month - date2.month
    return diff_year * 12 + diff_month


def extact_row(row:pd.Series, suffix:str):
    '''
    A function that extracts columns from a row based on a suffix and removes the suffix

    Args:
        df: Dataframe from where the row should be extracted from.
        index: The index of the row that will be extracted from.
        suffix: The suffix that will be searched for and removed
    '''
    row = row[[col for col in row.index if col.endswith(suffix) or col == 'ward_code']].copy()
    row = row.rename(
        {col:re.sub(suffix, '', col) for col in row.index}
    )

    return row

def expand_date_range(df:pd.DataFrame, date_col:str, ward_col:str):
    start_date, end_date = df[date_col].min(), df[date_col].max()
    month_delta = relativedelta(end_date, start_date)
    n_months = (month_delta.years * 12 + month_delta.months) 

    blank_rows = pd.DataFrame({col:[np.nan]*(n_months-1) for col in df.columns})

    month_col = [start_date+pd.offsets.MonthBegin(n=i) for i in range(1,len(blank_rows)+1)]
    blank_rows['date'] = month_col
    df = pd.concat([df.iloc[[0]], blank_rows, df.iloc[[1]]]).reset_index(drop=True)

    df[ward_col] = df.loc[0, ward_col]

    # df['year'] = df['date'].dt.year
    # df['month'] = df['date'].dt.month
    # df = df.drop('date', axis=1)
    
    return df

def interpolate_df(row:pd.DataFrame, columns_to_interpolate:List[str]|str):
    '''
    A function that given a dataframe with two rows, where each row has a date, it will generate new rows, interpolating any data between these two dates
    '''

    row[columns_to_interpolate] = row[columns_to_interpolate].apply(pd.to_numeric)

    if isinstance(columns_to_interpolate, str):
        columns_to_interpolate = str(columns_to_interpolate)

    for col in columns_to_interpolate:
        row[col] = row[col].interpolate()

    return row


def interpolate_forward(df:pd.DataFrame, date_col_name:str, cols_to_interpolate:List[str]|str, end_date:pd.Timestamp):
    '''
    
    '''
    largest_date_row = df.loc[df[date_col_name].idxmax()].to_dict()

    columns_to_copy = [col for col in df.columns if col not in [cols_to_interpolate, date_col_name]]
    new_row = {col:(largest_date_row[col] if col in columns_to_copy else np.nan) for col in df.columns}
    new_row[date_col_name] = end_date

    new_df = pd.DataFrame([largest_date_row, new_row])

    new_df = expand_date_range(new_df, 'date', 'ward_code')
    new_df = new_df.iloc[1:,:]

    df = pd.concat([df, new_df], ignore_index=True)
    df = interpolate_df(df, cols_to_interpolate)
    

    return df

