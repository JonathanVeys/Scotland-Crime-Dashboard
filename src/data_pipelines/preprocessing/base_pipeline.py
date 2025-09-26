from typing import Dict, List, Callable

import pandas as pd

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
    
    def normalise_column(self, normalise_func:Callable, col_to_normalise:str):
        self._check_list_subset(col_to_normalise, list(self.data.columns))

        # if not self.data[col_to_normalise].dtype == str:
        #     self.data[col_to_normalise] = self.data[col_to_normalise].astype(str)

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
    
