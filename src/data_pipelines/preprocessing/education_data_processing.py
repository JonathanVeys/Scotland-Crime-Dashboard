from typing import List, Literal
import re


import pandas as pd
from dateutil.relativedelta import relativedelta

from src.data_pipelines.preprocessing.utils import date_difference


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

def expand_date_range(row:pd.Series):
    '''
    Given a row with a start and end date, converts to a new dataframe, where each row is a month in that range, filling the missing data with NA.

    Args:
        df: The row with the data that needs to be expanded, it must contain a date_start column and a data_end column
    '''

    if 'date_start' not in row.index:
        raise ValueError('Error: "date_start" not found in column names.')
    if 'date_end' not in row.index:
        raise ValueError('Error: "date_end" not found in column names.')

    start, end = row['date_start'], row['date_end']
    month_delta = relativedelta(end, start)
    n_months = month_delta.years * 12 + month_delta.months 

    expanded_rows = []
    for month in range(1,n_months):
            new_row = {
                'ward_code':f'{row["ward_code"]}', 
                'date':start + pd.offsets.MonthBegin(n=month),
                'no_qual':pd.NA,
                'qual_level_1':pd.NA,
                'qual_level_2':pd.NA,
                'qual_level_3':pd.NA,
                'qual_level_4':pd.NA,
            }
            expanded_rows.append(new_row)

    start_row = extact_row(row, '_start').to_dict()
    end_row = extact_row(row, '_end').to_dict()

    expanded_rows.insert(0, start_row)
    expanded_rows.append(end_row) 
    
    return pd.DataFrame(expanded_rows)

def interpolate_df(row:pd.DataFrame, columns_to_interpolate:List[str]):
    '''
    A function that given a dataframe with two rows, where each row has a date, it will generate new rows, interpolating any data between these two dates
    '''

    row[columns_to_interpolate] = row[columns_to_interpolate].apply(pd.to_numeric)

    for col in columns_to_interpolate:
        row[col] = row[col].interpolate()

    return row

def combine_dict(dict1:dict, dict2:dict):
    if set(dict1.keys()) != set(dict2.keys()):
        raise ValueError('Dictionaries do not have matching keys')
    
    for key, values in dict2.items():
        dict1[key].extend(values)

    return dict1

