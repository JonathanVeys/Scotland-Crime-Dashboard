import os
import json
from dotenv import load_dotenv 
from typing import Dict, List   
from pathlib import Path

import pandas as pd
from pandera.pandas import DataFrameSchema, Column, DateTime
from pandas.api.types import is_numeric_dtype

from src.data_pipelines.preprocessing.spacial_processing import calculate_overlap, load_and_prepare_shapefile, apply_disambiguation
from src.data_pipelines.preprocessing.base_pipeline import BasePipeline
from src.data_pipelines.preprocessing.utils import expand_date_range, interpolate_df, normalise_text
from src.data_pipelines.DB.update_database import update_db
from src.data_pipelines.pipelines.mapping.mapping import WARD_2007_COL_RENAME_DICT, MANNUAL_WARD_NAME_EDITS_2007, WARD_2022_COL_RENAME_DICT, MANNUAL_WARD_NAME_EDITS_2022, NAME_DISAMBIGUATION_2007, NAME_DISAMBIGUATION_2022
import tests.test_crime_pipeline as tests

class EducationPipeline(BasePipeline):
    def __init__(self, path: str, skiprows:int=0, skipfooter:int=0):
        self.path = path
        self.skiprows = skiprows
        self.skipfooter = skipfooter
        self.data = self.load_data(skiprows=self.skiprows, skipfooter=self.skipfooter)
        super().__init__(self.data)

    def load_data(self, skiprows:int=0, skipfooter:int=0):
        '''
        Loads a csv file from memory
        '''
        data  = pd.read_csv(self.path, skiprows=skiprows, skipfooter=skipfooter, engine='python')
        return data

    def pivot_data(self, columns:str, values:str,  index:str):
        self._check_list_subset([columns, values, index], list(self.data.columns))

        self.data = self.data.reset_index()
        self.data = self.data.pivot(
            columns=columns,
            values=values,
            index=index
        ).reset_index()

        return self

    def calculate_percentages(self, division_map:Dict[str, str]):
        '''
        Calculates column percentages by giving a dictionary with column numerator and denominator pairs
        '''
        self._check_list_subset(list(division_map.keys()), list(self.data.columns))
        self._check_list_subset(list(division_map.values()), list(self.data.columns))


        for key, value in division_map.items():
            self.data[key] = self.divide_col(key, value)

        return self   

    def multiply_columns(self, mul_map:Dict[str, str]):
        for key, value in mul_map.items():
            self.data[key] = self.mul_cols(key, value)
        
        return self
    
    def groupby(self, grouping_columns:List[str]|str, summing_column:List[str]|str):
        self._check_list_subset(grouping_columns, list(self.data.columns))
        self._check_list_subset(summing_column, list(self.data.columns))

        self.data = self.data.groupby(grouping_columns)[summing_column].sum().reset_index()
        return self 
    
    def set_date_column(self, date_col_name:str, date:str):
        self.data[date_col_name] = pd.to_datetime(date)
        return self




def main():
    CURRENT_DIR = Path(__file__).resolve().parent
    PACKAGE_DIR = CURRENT_DIR.parent.parent.parent

    with open(PACKAGE_DIR / 'src/data_pipelines/pipelines/config/transformations.json') as f:
        config = json.load(f)
    education_data_2011_config = config['education_data_2011']
    education_data_2022_config = config['education_data_2022']


    ward_boundaries_2007_path = str(PACKAGE_DIR / 'data' / 'geojson_data' / '4th_Review_2007_2017_All_Scotland_wards' / 'All_Scotland_wards_4th.shp')
    ward_2007_geometry, ward_code_2007_lookup = load_and_prepare_shapefile(ward_boundaries_2007_path, 'ONS_2010', 'Name', '2007', 27700, normalise_text)

    ward_boundaries_2022_path = str(PACKAGE_DIR / 'data' / 'geojson_data' / 'scottish_wards_2022_shapefile' / 'Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp')
    ward_2022_geometry, ward_code_2022_lookup = load_and_prepare_shapefile(ward_boundaries_2022_path, 'WD25CD', 'WD25NM', '2022', 27700, normalise_text)

    ward_2007_2022_map = calculate_overlap(ward_2007_geometry, ward_2022_geometry)
    ward_2007_2022_map = ward_2007_2022_map[['ward_code_2007', 'ward_code_2022', 'overlap_pct']]

    ward_code_2007_lookup = apply_disambiguation(ward_code_2007_lookup, 'ward_code_2007', 'ward_name_2007', NAME_DISAMBIGUATION_2007)
    ward_code_2022_lookup = apply_disambiguation(ward_code_2022_lookup, 'ward_code_2022', 'ward_name_2022', NAME_DISAMBIGUATION_2022)


    education_data_2011_path = str(PACKAGE_DIR / education_data_2011_config['path'])
    education_data_2011 = EducationPipeline(education_data_2011_path, skiprows=12, skipfooter=5)
    education_data_2011 = (
        education_data_2011.pivot_data(columns='Highest level of qualification', values='Count', index='Electoral Ward 2007')
        .rename_cols(education_data_2011_config['transformations']['column_rename'])
        .normalise_column(normalise_text, col_to_normalise='ward_name_2007')
        .apply_manual_edits(col='ward_name_2007', mannual_edits=education_data_2011_config['transformations']['mannual_ward_edits'])
        .left_join(ward_code_2007_lookup, 'ward_name_2007')
        .sum_cols('pop_with_qual', ['qual_level_3', 'qual_level_4'])
        .sum_cols('pop_without_qual', ['qual_level_1', 'qual_level_2', 'no_qual'])
        .calculate_percentages({col:'total_population' for col in ['pop_with_qual', 'pop_without_qual']})
        .drop_columns([col for col in education_data_2011.data.columns if col not in ['ward_code_2007', 'pop_with_qual', 'pop_without_qual']])
        .left_join(pd.DataFrame(ward_2007_2022_map), 'ward_code_2007')
        .multiply_columns(mul_map={col:'overlap_pct' for col in education_data_2011.data.columns if is_numeric_dtype(education_data_2011.data[col])})
        .groupby('ward_code_2022', ['pop_with_qual', 'pop_without_qual'])
        .set_date_column('date', '2011-01-01')
        .extract_df()
    )

    education_data_2022_path = str(PACKAGE_DIR / 'data' / 'education_data' / 'education_ward_data_2022.csv')
    education_data_2022 = EducationPipeline(education_data_2022_path, skiprows=10, skipfooter=8)
    education_data_2022 = (
        education_data_2022.pivot_data(columns='Highest level of qualification', values='Count', index='Electoral Ward 2022')
        .rename_cols(education_data_2022_config['transformations']['column_rename'])
        .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2022')
        .apply_manual_edits(col='ward_name_2022', mannual_edits=education_data_2022_config['transformations']['mannual_ward_edits'])
        .left_join(ward_code_2022_lookup, merging_column='ward_name_2022')
        .sum_cols('pop_with_qual', ['qual_level_3', 'qual_level_3.5', 'qual_level_4'])
        .sum_cols('pop_without_qual', ['qual_level_1', 'qual_level_2', 'no_qual'])
        .calculate_percentages({col:'total_population' for col in ['pop_with_qual', 'pop_without_qual']})
        .drop_columns([col for col in education_data_2022.data.columns if col not in ['ward_code_2022', 'pop_with_qual', 'pop_without_qual']])
        .set_date_column('date', '2022-01-01')
        .extract_df()   
    )


    education_data = pd.concat([education_data_2011, education_data_2022], ignore_index=True)
    
    education_data_list = []
    for _, group_df in education_data.groupby(['ward_code_2022']):
        df = expand_date_range(group_df, 'date', 'ward_code_2022')
        df = interpolate_df(df, ['pop_with_qual', 'pop_without_qual'])
        education_data_list.append(df)
    education_data = pd.concat(education_data_list, ignore_index=True)
    education_data = education_data.rename(columns={'ward_code_2022':'ward_code'})

    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'date',
        'pop_with_qual',
        'pop_without_qual',
    ]

    schema = DataFrameSchema({
        'ward_code':Column(str),
        'pop_with_qual':Column(float),
        'pop_without_qual':Column(float),
        'date':Column(DateTime, nullable=False)
    })


    tests.test_timeseries(education_data, start_date='2011-01-01', end_date='2022-01-01', ward_code_col='ward_code')
    tests.test_NA(education_data, cols=['ward_code', 'pop_with_qual', 'pop_without_qual', 'date'])
    tests.test_columns_exist(education_data, cols=['ward_code', 'pop_with_qual', 'pop_without_qual', 'date'])
    tests.test_schema(education_data, schema=schema)

    if DB_URL is not None:
        update_db(data=education_data, db_url=DB_URL, table_name='ward_education_data', required_columns=required_columns)




if __name__ == '__main__':
    main()








