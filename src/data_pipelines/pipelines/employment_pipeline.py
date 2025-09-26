from typing import Dict, List
from dotenv import load_dotenv   
from pathlib import Path  
import os 
import json


import pandas as pd
from pandas.api.types import is_numeric_dtype

from src.data_pipelines.preprocessing.spacial_processing import calculate_overlap, load_and_prepare_shapefile, apply_disambiguation
from src.data_pipelines.preprocessing.base_pipeline import BasePipeline
from src.data_pipelines.preprocessing.utils import normalise_text, expand_date_range, interpolate_df
from src.data_pipelines.pipelines.mapping.employment_mapping import NAME_DISAMBIGUATION_2007, NAME_DISAMBIGUATION_2022
from src.data_pipelines.DB.update_database import update_db



class EmploymentPipeline(BasePipeline):
    def __init__(self, path: str, skiprows:int=0, skipfooter:int=0):
        self.path = path
        self.skiprows = skiprows
        self.skipfooter = skipfooter
        self.data = self.load_data()
        super().__init__(self.data)

    def load_data(self):
        '''
        Loads a csv file from memory
        '''
        data  = pd.read_csv(self.path, skiprows=self.skiprows, skipfooter=self.skipfooter, engine='python')
        return data

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



def main():
    CURRENT_DIR = Path(__file__).resolve().parent
    PACKAGE_DIR = CURRENT_DIR.parent.parent.parent

    with open(PACKAGE_DIR / 'src/data_pipelines/pipelines/config/transformations.json') as f:
        config = json.load(f)
    employment_data_2011_config = config['employment_data_2011']
    employment_data_2022_config = config['employment_data_2022']

    ward_boundaries_2007_path = str(PACKAGE_DIR / 'data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp')
    ward_2007_geometry, ward_code_2007_lookup = load_and_prepare_shapefile(ward_boundaries_2007_path, 'ONS_2010', 'Name', '2007', 27700, normalise_text)

    ward_boundaries_2022_path = str(PACKAGE_DIR / 'data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp')
    ward_2022_geometry, ward_code_2022_lookup = load_and_prepare_shapefile(ward_boundaries_2022_path, 'WD25CD', 'WD25NM', '2022', 27700, normalise_text)

    ward_2007_2022_map = calculate_overlap(ward_2007_geometry, ward_2022_geometry)
    ward_2007_2022_map = ward_2007_2022_map[['ward_code_2007', 'ward_code_2022', 'overlap_pct']]

    ward_code_2007_lookup = apply_disambiguation(ward_code_2007_lookup, 'ward_code_2007', 'ward_name_2007', NAME_DISAMBIGUATION_2007)
    ward_code_2022_lookup = apply_disambiguation(ward_code_2022_lookup, 'ward_code_2022', 'ward_name_2022', NAME_DISAMBIGUATION_2022)


    employment_2011_path = PACKAGE_DIR / employment_data_2011_config['path']
    employment_data_2011 = EmploymentPipeline(employment_2011_path, skiprows=12, skipfooter=5)
    employment_data_2011 = (
        employment_data_2011.rename_cols(employment_data_2011_config['transformations']['column_rename'])                                                                                                   #Rename columns
        .sum_cols('economically_active_adults', [col for col in employment_data_2011.data.columns if 'economically active' in col.lower()])     #Sum columns to find total economically active adults
        .calculate_percentages({'caring_for_family':'total_pop', 'long_term_sick_or_disabled':'total_pop', 'unemployed_adults':'economically_active_adults'})                                                                                            #Calculate column percentages
        .subset_columns(['ward_name_2007', 'unemployed_adults', 'long_term_sick_or_disabled', 'caring_for_family'])                             #Select a subset of columns from the data
        .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2007')                                                     #Normalise the ward name column
        .apply_manual_edits('ward_name_2007', employment_data_2011_config['transformations']['mannual_ward_edits'])                                                                     #Apply the mannual edits dictionary
        .left_join(data_to_merge=ward_code_2007_lookup, merging_column='ward_name_2007')                                                        #Left join the data with a ward name to ward code lookup
        .drop_columns('ward_name_2007')                                                                                                         #Drop the ward name column
        .left_join(data_to_merge=pd.DataFrame(ward_2007_2022_map), merging_column='ward_code_2007')
        .mulitply_columns(mul_map={col:'overlap_pct' for col in employment_data_2011.data.columns if is_numeric_dtype(employment_data_2011.data[col])})
        .groupby(['ward_code_2022'], ['unemployed_adults', 'long_term_sick_or_disabled', 'caring_for_family'])
        .rename_cols({'ward_code_2022':'ward_code'})
        .extract_df()
    )
    
    employment_2022_path = PACKAGE_DIR / employment_data_2022_config['path']
    employment_data_2022 = EmploymentPipeline(employment_2022_path, skiprows=8, skipfooter=5)
    employment_data_2022 = (
        employment_data_2022.rename_cols(employment_data_2022_config['transformations']['column_rename'])
        .sum_cols('economically_active_adults', ['Economically Active (excluding full-time students) - Total', 'Economically Active full-time students - Total'])
        .sum_cols('unemployed_adults', ['Economically Active (excluding full-time students) - Unemployed - Available for work', 'Economically Active full-time students - Unemployed - Available for work'])
        .calculate_percentages({'caring_for_family':'total_pop', 'long_term_sick_or_disabled':'total_pop', 'unemployed_adults':'economically_active_adults'})
        .subset_columns(['ward_name_2022', 'caring_for_family', 'long_term_sick_or_disabled', 'unemployed_adults'])
        .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2022')
        .apply_manual_edits('ward_name_2022', employment_data_2022_config['transformations']['mannual_ward_edits'])
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
    employment_data = employment_data[['ward_code', 'date', 'unemployed_adults', 'long_term_sick_or_disabled', 'caring_for_family']]

    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'unemployed_adults',
        'long_term_sick_or_disabled',
        'caring_for_family',
        'date'
    ]

    if DB_URL is not None:
        update_db(data=employment_data, db_url=DB_URL, table_name='ward_employemnt_data', required_columns=required_columns)






if __name__ == '__main__':
    main()

