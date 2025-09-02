from typing import Dict, List, Callable

import pandas as pd

from src.data_pipelines.preprocessing.spacial_processing import calculate_overlap, load_and_prepare_shapefile, apply_disambiguation
from src.data_pipelines.preprocessing.utils import normalise_text, rename_column_names, subset_columns
from src.data_pipelines.employment_mapping import NAME_DISAMBIGUATION_2007, NAME_DISAMBIGUATION_2022, MANNUAL_WARD_NAME_EDITS_2007


class EmploymentPipeline:
    def __init__(self, path:str, ward_name_col:str):
        self.path = path
        self.ward_name_col = ward_name_col
        
    def load_data(self, skiprows:int=0, skipfooter:int=0):
        '''
        Loads a csv file from memory
        '''
        self.data  = pd.read_csv(self.path, skiprows=skiprows, skipfooter=skipfooter)
        return self
    
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
    
    def sum_cols(self, new_col_name:str, cols_to_add:List[str]):
        '''
        Adds a series of columns in a dataframe rowise
        '''
        self._check_list_subset(cols_to_add, list(self.data.columns))

        self.data[new_col_name] = self.data[cols_to_add].sum(axis=1)
        return self
    
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
    
    def apply_mannual_edits(self, col:str, mannual_edits:Dict[str,str]):
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
        

    
COLUMN_RENAME_DICT = {
            'Electoral Ward 2007':'ward_name_2007',
            'All people aged 16 to 74':'total_pop',
            'Economically inactive: Looking after home or family':'caring_for_family',
            'Economically inactive: Long-term sick or disabled':'long_term_sick_or_disabled',
            'Unemployed people aged 16 to 74: Aged 16 to 24':'young_unemployed'
    }

def main():
    ward_boundaries_2007_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp'
    ward_2007_geometry, ward_code_2007_lookup = load_and_prepare_shapefile(ward_boundaries_2007_path, 'ONS_2010', 'Name', '2007', 27700, normalise_text)

    ward_boundaries_2022_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp'
    ward_2022_geometry, ward_code_2022_lookup = load_and_prepare_shapefile(ward_boundaries_2022_path, 'WD25CD', 'WD25NM', '2022', 27700, normalise_text)

    ward_2007_2022_map = calculate_overlap(ward_2007_geometry, ward_2022_geometry)
    ward_2007_2022_map = ward_2007_2022_map[['ward_code_2007', 'ward_code_2022', 'overlap_pct']]

    ward_code_2007_lookup = apply_disambiguation(ward_code_2007_lookup, 'ward_code_2007', 'ward_name_2007', NAME_DISAMBIGUATION_2007)
    ward_code_2022_lookup = apply_disambiguation(ward_code_2022_lookup, 'ward_code_2022', 'ward_name_2022', NAME_DISAMBIGUATION_2022)

    employment_2011_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/employment_data/Census_Employment_2011.csv'
    employment_data = EmploymentPipeline(employment_2011_path, 'Electoral Ward 2007')
    employment_data = (
        employment_data.load_data(skiprows=12, skipfooter=5)
        .rename_cols(COLUMN_RENAME_DICT)
        .sum_cols('employed_adults', ['Economically active: Employee: Full-time', 'Economically active: Self-employed', 'Economically active: Unemployed'])
        .divide_cols(new_col_name='caring_for_family_pct', col_numerator_name='caring_for_family', col_denominator_name='total_pop')
        .divide_cols(new_col_name='long_term_sick_or_disabled_pct', col_numerator_name='long_term_sick_or_disabled', col_denominator_name='total_pop')
        .divide_cols(new_col_name='young_unemployed_pct', col_numerator_name='young_unemployed', col_denominator_name='total_pop')
        .divide_cols(new_col_name='unemployment_pct', col_numerator_name='Economically active: Unemployed', col_denominator_name='employed_adults')
        .subset_columns(['ward_name_2007', 'unemployment_pct', 'young_unemployed_pct', 'long_term_sick_or_disabled_pct', 'caring_for_family_pct'])
        .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2007')
        .apply_mannual_edits('ward_name_2007', MANNUAL_WARD_NAME_EDITS_2007)
        .left_join(data_to_merge=ward_code_2007_lookup, merging_column='ward_name_2007')
        .drop_columns('ward_name_2007')
    )
    
    print(employment_data.data)


def main2():
    ward_boundaries_2007_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp'
    ward_2007_geometry, ward_code_2007_lookup = load_and_prepare_shapefile(ward_boundaries_2007_path, 'ONS_2010', 'Name', '2007', 27700, normalise_text)

    ward_boundaries_2022_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp'
    ward_2022_geometry, ward_code_2022_lookup = load_and_prepare_shapefile(ward_boundaries_2022_path, 'WD25CD', 'WD25NM', '2022', 27700, normalise_text)

    ward_2007_2022_map = calculate_overlap(ward_2007_geometry, ward_2022_geometry)
    ward_2007_2022_map = ward_2007_2022_map[['ward_code_2007', 'ward_code_2022', 'overlap_pct']]

    ward_code_2007_lookup = apply_disambiguation(ward_code_2007_lookup, 'ward_code_2007', 'ward_name_2007', NAME_DISAMBIGUATION_2007)
    ward_code_2022_lookup = apply_disambiguation(ward_code_2022_lookup, 'ward_code_2022', 'ward_name_2022', NAME_DISAMBIGUATION_2022)




    employment_2011_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/employment_data/Census_Employment_2011.csv'
    employment_2011 = pd.read_csv(employment_2011_path, skiprows=12, skipfooter=3)
    
    employment_cols = ['Economically active: Employee: Full-time', 'Economically active: Self-employed', 'Economically active: Unemployed']
    unemployment_cols = ['Economically active: Unemployed']

    employment_2011['unemployment_pct'] = employment_2011[unemployment_cols].sum(axis=1) / employment_2011[employment_cols].sum(axis=1)

    COLUMN_RENAME_DICT = {
            'Economic activity then Unemployed people aged 16 to 74 then Unemployed people aged 16 to 74, Never worked and long-term unemployed':'ward_name_2007',
            'All people aged 16 to 74':'total_pop',
            'Economically inactive: Looking after home or family':'caring_for_family',
            'Economically inactive: Long-term sick or disabled':'long_term_sick_or_disabled',
            'Unemployed people aged 16 to 74: Aged 16 to 24':'young_unemployed',
            'unemployment_pct':'unemployment_pct'
    }

    employment_2011 = rename_column_names(
        employment_2011,
        rename_dict=COLUMN_RENAME_DICT
    )
    employment_2011 = subset_columns(employment_2011, list(COLUMN_RENAME_DICT.values()))
    
    for col in ['caring_for_family', 'long_term_sick_or_disabled', 'young_unemployed']:
        employment_2011[col] = employment_2011[col].div(employment_2011['total_pop'])

    employment_2011 = employment_2011.drop('total_pop', axis=1)
    employment_2011['ward_name_2007'] = employment_2011['ward_name_2007'].apply(normalise_text)

    employment_2011['ward_name_2007'] = employment_2011['ward_name_2007'].map(
        lambda x:MANNUAL_WARD_NAME_EDITS_2007.get(x,x)
    )
    employment_2011 = employment_2011.merge(ward_code_2007_lookup, on='ward_name_2007', how='left').drop('ward_name_2007', axis=1)

    employment_2011['date_start'] = pd.to_datetime('2011-01-01')

    print(employment_2011.sort_values('young_unemployed', ascending=False))




if __name__ == '__main__':
    main()

