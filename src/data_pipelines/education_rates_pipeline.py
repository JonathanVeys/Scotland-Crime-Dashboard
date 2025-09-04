import os
from dotenv import load_dotenv 
from typing import Dict, List   

import pandas as pd
from pandas.api.types import is_numeric_dtype

from src.data_pipelines.preprocessing.spacial_processing import calculate_overlap, load_and_prepare_shapefile, apply_disambiguation
from src.data_pipelines.preprocessing.utils import BasePipeline, expand_date_range, interpolate_df, normalise_text
from src.data_pipelines.DB.update_database import update_db
from src.data_pipelines.mapping import WARD_2007_COL_RENAME_DICT, MANNUAL_WARD_NAME_EDITS_2007, WARD_2022_COL_RENAME_DICT, MANNUAL_WARD_NAME_EDITS_2022, NAME_DISAMBIGUATION_2007, NAME_DISAMBIGUATION_2022


class EducationPipeline(BasePipeline):
    def __init__(self, path: str):
        super().__init__(path)

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

    def mulitply_columns(self, mul_map:Dict[str, str]):
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


EDUCATION_DATA_2011_COLUMN_RENAME={
    'ward_code_2007':'ward_code_2007',
    'All people aged 16 and over: Level 1':'qual_level_1',
    'All people aged 16 and over: Level 2':'qual_level_2',
    'All people aged 16 and over: Level 3':'qual_level_3',
    'All people aged 16 and over: Level 4 and above':'qual_level_4',
    'All people aged 16 and over: No qualifications':'no_qual',
    'All people aged 16 and over: Total':'total_population'
}

EDUCATION_DATA_2022_COLUMN_RENAME={
    'ward_code_2022':'ward_code_2022',
    'All people aged 16 and over':'total_population',
    'No qualifications':'no_qual',
    'Lower school qualifications':'qual_level_1',
    'Upper school qualifications':'qual_level_2',
    'Further Education and sub-degree Higher Education qualifications incl. HNC/HNDs':'qual_level_3',
    'Apprenticeship qualifications':'qual_level_3.5',
    'Degree level qualifications or above':'qual_level_4'
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


    education_data_2011_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/education_data/education_ward_data_2011.csv'
    education_data_2011 = EducationPipeline(education_data_2011_path)
    education_data_2011 = (
        education_data_2011.load_data(skiprows=12, skipfooter=5)
        .rename_cols(WARD_2007_COL_RENAME_DICT)
        .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2007')
        .apply_mannual_edits(col='ward_name_2007', mannual_edits=MANNUAL_WARD_NAME_EDITS_2007)
        .subset_columns(column_subset_list=list(WARD_2007_COL_RENAME_DICT.values()))
        .left_join(data_to_merge=ward_code_2007_lookup, merging_column='ward_name_2007')
        .drop_columns('ward_name_2007')
        .pivot_data(columns='qualification', values='count', index='ward_code_2007')
        .rename_cols(EDUCATION_DATA_2011_COLUMN_RENAME)
        .calculate_percentages({col:'total_population' for col in ['qual_level_1', 'qual_level_2', 'qual_level_3', 'qual_level_4', 'no_qual']})
        .drop_columns('total_population')
        .left_join(pd.DataFrame(ward_2007_2022_map), 'ward_code_2007')
        .mulitply_columns(mul_map={col:'overlap_pct' for col in education_data_2011.data.columns if is_numeric_dtype(education_data_2011.data[col])})
        .groupby('ward_code_2022', ['qual_level_1', 'qual_level_2', 'qual_level_3', 'qual_level_4', 'no_qual'])
        .set_date_column('date', '2011-01-01')
        .extract_df()
    )

    education_data_2022_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/education_data/eductaion_ward_data_2022.csv'
    education_data_2022 = EducationPipeline(education_data_2022_path)
    education_data_2022 = (
        education_data_2022.load_data(skiprows=10, skipfooter=8)
        .rename_cols(WARD_2022_COL_RENAME_DICT)
        .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2022')
        .apply_mannual_edits(col='ward_name_2022', mannual_edits=MANNUAL_WARD_NAME_EDITS_2022)
        .subset_columns(column_subset_list=list(WARD_2022_COL_RENAME_DICT.values()))
        .left_join(data_to_merge=ward_code_2022_lookup, merging_column='ward_name_2022')
        .pivot_data(columns='qualification', values='count', index='ward_code_2022')
        .rename_cols(EDUCATION_DATA_2022_COLUMN_RENAME)
        .sum_cols('qual_level_3', ['qual_level_3', 'qual_level_3.5'])
        .drop_columns('qual_level_3.5')
        .calculate_percentages({col:'total_population' for col in ['qual_level_1', 'qual_level_2', 'qual_level_3', 'qual_level_4', 'no_qual']})
        .drop_columns('total_population')
        .set_date_column('date', '2022-01-01')
        .extract_df()
    )

    education_data = pd.concat([education_data_2011, education_data_2022], ignore_index=True)
    
    education_data_list = []
    for _, group_df in education_data.groupby(['ward_code_2022']):
        df = expand_date_range(group_df, 'date', 'ward_code_2022')
        df = interpolate_df(df, ['qual_level_1', 'qual_level_2', 'qual_level_3', 'qual_level_4', 'no_qual'])
        education_data_list.append(df)
    education_data = pd.concat(education_data_list, ignore_index=True)
    education_data = education_data.rename(columns={'ward_code_2022':'ward_code'})
    
    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'year',
        'month',
        'qual_level_1',
        'qual_level_2',
        'qual_level_3',
        'qual_level_4',
        'no_qual',
    ]

    if DB_URL is not None:
        update_db(data=education_data, db_url=DB_URL, table_name='ward_education_data', required_columns=required_columns)





if __name__ == '__main__':
    main()








