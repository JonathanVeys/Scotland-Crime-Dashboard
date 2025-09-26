import pytest
import pandas as pd
import pandera.pandas as pa
import pandera.errors as pr

from typing import List

class PipelineTestError(Exception):
    '''
    Custom exception for pipeline test failures
    '''
    pass


def test_columns_exist(df:pd.DataFrame, cols:List[str]):
    '''
    A test to check that all necesary columns are present in a dataframe
    '''
    missing = [col for col in cols if col not in df.columns]
    if missing:
        raise PipelineTestError(f'Error: Missing columns = {missing}')
    else:
        print('✅test_column_exists passed succesfully')


def test_NA(df:pd.DataFrame, cols:List[str]):
    '''
    A test to check if there any any NA values in the columns of a dataframe
    '''
    for col in cols:
        if df[col].isna().any():
            raise PipelineTestError(f'Error: There are NA values in column = {col}')
    print('✅test_NA passed successfully')


def test_schema(df:pd.DataFrame, schema:pa.DataFrameSchema):
    '''
    A test that checks a dataframe against a schema to ensure the correct types for the columns
    '''

    try:
        schema.validate(df)
    except pr.SchemaError as e:
        raise(PipelineTestError(f'Error: Schema failed to validate - {e}'))
    else:
        print('✅test_schema passed successfully')


def test_timeseries(df:pd.DataFrame, start_date:str, end_date:str, ward_code_col:str):
    '''
    A test to ensure that for each ward code in the data, we have a complete timeseries
    '''
    
    for _,ward_df in df.groupby([ward_code_col]):
        date_incrementor = pd.to_datetime(start_date)
        for index, row in ward_df.iterrows():
            if row['date'] != date_incrementor:
                raise PipelineTestError(f"Timeseries broken on row: {index}, timeseries date: {row['date'].date()} does not match expected date: {date_incrementor.date()}")
            date_incrementor = date_incrementor + pd.DateOffset(months=1)

    print('✅test_timeseries passed successfully')

        


