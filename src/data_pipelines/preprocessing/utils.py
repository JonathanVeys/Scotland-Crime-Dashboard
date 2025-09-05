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

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df = df.drop('date', axis=1)
    
    return df

def interpolate_df(row:pd.DataFrame, columns_to_interpolate:List[str]):
    '''
    A function that given a dataframe with two rows, where each row has a date, it will generate new rows, interpolating any data between these two dates
    '''

    row[columns_to_interpolate] = row[columns_to_interpolate].apply(pd.to_numeric)

    for col in columns_to_interpolate:
        row[col] = row[col].interpolate()

    return row

class BasePipeline:
    def __init__(self, data:pd.DataFrame):
        self.data = data
    
    
    def rename_cols(self, rename_map:Dict[str,str]):
        '''
        Renames the columns in a dataframe using a dictionary
        '''
        self._check_list_subset(list(rename_map.keys()), list(self.data.columns))
        
        self.data = self.data.rename(
            columns=rename_map
        )
        return self
    
    def divide_cols(self, new_col_name:str, col_numerator_name:str, col_denominator_name:str):
        '''
        Divides two columns in a dataframe rowise
        '''
        self._check_list_subset([col_numerator_name, col_denominator_name], list(self.data.columns))

        self.data[new_col_name] = self.data[col_numerator_name].div(self.data[col_denominator_name])
        return self
    
    def divide_col(self, col_numerator_name:str, col_denominator_name:str):
        '''
        
        '''
        self._check_list_subset([col_numerator_name, col_denominator_name], list(self.data.columns))

        new_col = self.data[col_numerator_name].div(self.data[col_denominator_name])
        return new_col
    
    def sum_cols(self, new_col_name:str, cols_to_add:List[str]):
        '''
        Adds a series of columns in a dataframe rowise
        '''
        self._check_list_subset(cols_to_add, list(self.data.columns))

        self.data[new_col_name] = self.data[cols_to_add].sum(axis=1)
        return self
    
    def mul_cols(self, col1_name:str, col2_name:str):
        self._check_list_subset([col1_name, col2_name], list(self.data.columns))

        new_col = self.data[col1_name].mul(self.data[col2_name])
        return new_col
    
    def subset_columns(self, column_subset_list:List[str]):
        '''
        Subsets a dataframe by selecting columns
        '''
        self._check_list_subset(column_subset_list, list(self.data.columns))

        self.data = self.data[column_subset_list]
        return self
    
    def normalise_column(self, normalise_func:Callable[[str], str], col_to_normalise:str):
        self._check_list_subset(col_to_normalise, list(self.data.columns))

        if not self.data[col_to_normalise].dtype == str:
            self.data[col_to_normalise] = self.data[col_to_normalise].astype(str)

        self.data[col_to_normalise] = self.data[col_to_normalise].apply(normalise_func)
        return self
    
    def left_join(self, data_to_merge:pd.DataFrame, merging_column:str):
        self._check_list_subset(merging_column, list(self.data.columns))
        self._check_list_subset(merging_column, list(data_to_merge.columns))

        if not isinstance(data_to_merge, pd.DataFrame):
            raise ValueError(f'Expectetd pd.DataFrame, instead got {type(data_to_merge).__name__}')
        
        self.data = self.data.merge(data_to_merge, on=merging_column, how='left')
        return self
    
    def apply_manual_edits(self, col:str, mannual_edits:Dict[str,str]):
        self._check_list_subset(col, list(self.data.columns))

        self.data[col] = self.data[col].map(
            lambda x:mannual_edits.get(x,x)
        )
        return self
    
    def drop_columns(self, columns_to_drop:List[str]|str):
        self._check_list_subset(columns_to_drop, list(self.data.columns))

        self.data = self.data.drop(columns_to_drop, axis=1)
        return self
    

    def _check_list_subset(self, columns_to_check:List[str]|str, columns_list:List[str]|str):
        '''
        A helper function for checking if one list is a subset of another list
        '''
        if isinstance(columns_to_check, str):
            columns_to_check = [columns_to_check]

        missing_cols = [col for col in columns_to_check if col not in columns_list]
        if missing_cols:
            raise ValueError(f'{missing_cols} were expected in column names but were missing')
        

    def extract_df(self):
        '''
        A function for extracting the data from the class
        '''
        return self.data