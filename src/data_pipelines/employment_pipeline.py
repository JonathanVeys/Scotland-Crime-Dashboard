from typing import Dict, List
from dotenv import load_dotenv     
import os 


import pandas as pd
from pandas.api.types import is_numeric_dtype

from src.data_pipelines.preprocessing.spacial_processing import calculate_overlap, load_and_prepare_shapefile, apply_disambiguation
from src.data_pipelines.preprocessing.utils import normalise_text, rename_column_names, subset_columns, BasePipeline, expand_date_range, interpolate_df
from src.data_pipelines.employment_mapping import NAME_DISAMBIGUATION_2007, NAME_DISAMBIGUATION_2022, MANNUAL_WARD_NAME_EDITS_2007, MANNUAL_WARD_NAME_EDITS_2022
from src.data_pipelines.DB.update_database import update_db



class EmploymentPipeline(BasePipeline):
    def __init__(self, path: str):
        super().__init__(path)

    def calculate_percentages(self, division_map:Dict[str, str]):
        '''
        Calculates column percentages by giving a dictionary with column numerator and denominator pairs
        '''
        self._check_list_subset(list(division_map.keys()), list(self.data.columns))
        self._check_list_subset(list(division_map.values()), list(self.data.columns))


        for key, value in division_map.items():
            self.data[key] = self.divide_col(key, value)

        return self
    
    def mulitply_columns(self, mul_map:Dict[str, str]):
        for key, value in mul_map.items():
            self.data[key] = self.mul_cols(key, value)
        
        return self
    
    def groupby(self, grouping_columns:List[str]|str, summing_column:List[str]|str):
        self._check_list_subset(grouping_columns, list(self.data.columns))
        self._check_list_subset(summing_column, list(self.data.columns))

        self.data = self.data.groupby(grouping_columns)[summing_column].sum().reset_index()
        return self
    
COLUMN_RENAME_DICT_2007 = {
    'Electoral Ward 2007':'ward_name_2007',
    'All people aged 16 to 74':'total_pop',
    'Economically inactive: Looking after home or family':'caring_for_family',
    'Economically inactive: Long-term sick or disabled':'long_term_sick_or_disabled',
    'Economically active: Unemployed':'unemployed_adults'
}

COLUMN_RENAME_DICT_2022 = {
    'Economic activity - 20 groups, all':'ward_name_2022',
    'All people aged 16 and over':'total_pop',
    'Economically inactive - Looking after home/ family':'caring_for_family',
    'Economically inactive - Long term sick or disabled':'long_term_sick_or_disabled',
}

percentage_cols_2011 = {
    'caring_for_family':'total_pop',
    'long_term_sick_or_disabled':'total_pop',
    'unemployed_adults':'economically_active_adults'
}

percentage_cols_2022 = {
    'caring_for_family':'total_pop',
    'long_term_sick_or_disabled':'total_pop',
    'unemployed_adults':'economically_active_adults'
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
    employment_data_2011 = EmploymentPipeline(employment_2011_path)
    employment_data_2011 = (
        employment_data_2011.load_data(skiprows=12, skipfooter=5)
        .rename_cols(COLUMN_RENAME_DICT_2007)                                                                                                   #Rename columns
        .sum_cols('economically_active_adults', [col for col in employment_data_2011.data.columns if 'economically active' in col.lower()])     #Sum columns to find total economically active adults
        .calculate_percentages(percentage_cols_2011)                                                                                            #Calculate column percentages
        .subset_columns(['ward_name_2007', 'unemployed_adults', 'long_term_sick_or_disabled', 'caring_for_family'])                #Select a subset of columns from the data
        .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2007')                                                     #Normalise the ward name column
        .apply_mannual_edits('ward_name_2007', MANNUAL_WARD_NAME_EDITS_2007)                                                                    #Apply the mannual edits dictionary
        .left_join(data_to_merge=ward_code_2007_lookup, merging_column='ward_name_2007')                                                        #Left join the data with a ward name to ward code lookup
        .drop_columns('ward_name_2007')                                                                                                         #Drop the ward name column
        .left_join(data_to_merge=pd.DataFrame(ward_2007_2022_map), merging_column='ward_code_2007')
        .mulitply_columns(mul_map={col:'overlap_pct' for col in employment_data_2011.data.columns if is_numeric_dtype(employment_data_2011.data[col])})
        .groupby(['ward_code_2022'], ['unemployed_adults', 'long_term_sick_or_disabled', 'caring_for_family'])
        .rename_cols({'ward_code_2022':'ward_code'})
        .extract_df()
    )
    
    employment_2022_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/employment_data/Census_Employment_2022.csv'
    employment_data_2022 = EmploymentPipeline(employment_2022_path)
    employment_data_2022 = (
        employment_data_2022.load_data(skiprows=8, skipfooter=5)
        .rename_cols(COLUMN_RENAME_DICT_2022)
        .sum_cols('economically_active_adults', ['Economically Active (excluding full-time students) - Total', 'Economically Active full-time students - Total'])
        .sum_cols('unemployed_adults', ['Economically Active (excluding full-time students) - Unemployed - Available for work', 'Economically Active full-time students - Unemployed - Available for work'])
        .calculate_percentages(percentage_cols_2022)
        .subset_columns(['ward_name_2022', 'caring_for_family', 'long_term_sick_or_disabled', 'unemployed_adults'])
        .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2022')
        .apply_mannual_edits('ward_name_2022', MANNUAL_WARD_NAME_EDITS_2022)
        .left_join(data_to_merge=ward_code_2022_lookup, merging_column='ward_name_2022')
        .drop_columns('ward_name_2022')
        .rename_cols({'ward_code_2022':'ward_code'})
        .extract_df()
    )

    employment_data_2011['date'] = pd.to_datetime('2011-01-01')
    employment_data_2022['date'] = pd.to_datetime('2022-01-01')
    employment_data = pd.concat([employment_data_2011, employment_data_2022], ignore_index=True)
    
    employment_data_list = []
    for _, group_df in employment_data.groupby(['ward_code']):
        df = expand_date_range(group_df.reset_index(drop=True), 'date', 'ward_code')
        df = interpolate_df(df, ['unemployed_adults', 'long_term_sick_or_disabled', 'caring_for_family'])
        employment_data_list.append(df)
    employment_data = pd.concat(employment_data_list, ignore_index=True)
    employment_data = employment_data[['ward_code', 'year', 'month', 'unemployed_adults', 'long_term_sick_or_disabled', 'caring_for_family']]

    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'unemployed_adults',
        'long_term_sick_or_disabled',
        'caring_for_family',
        'year',
        'month'
    ]

    if DB_URL is not None:
        update_db(data=employment_data, db_url=DB_URL, table_name='ward_employemnt_data', required_columns=required_columns)






if __name__ == '__main__':
    main()

