import os
from typing import List
from dotenv import load_dotenv 

import pandas as pd

from src.data_pipelines.scraping.crime_scrapper import get_crime_data_url, crime_data_scrapper
from src.data_pipelines.preprocessing.utils import normalise_text, subset_columns, rename_column_names, BasePipeline
from src.data_pipelines.preprocessing.spacial_processing import calculate_overlap, load_and_prepare_shapefile, apply_disambiguation
from src.data_pipelines.DB.update_database import update_db

from src.data_pipelines.pipelines.mapping.crime_rates_mapping import MANNUAL_WARD_NAME_EDITS, NAME_DISAMBIGUATION_2007, NAME_DISAMBIGUATION_2022


class CrimePipeline(BasePipeline):
    def __init__(self, data:pd.DataFrame):
        self.data = data
        super().__init__(self.data)

    def filter_df(self, filter_col:str, filter_value:List[str]):
        self._check_list_subset(filter_col, list(self.data.columns))

        self.data = self.data[~self.data[filter_col].isin(filter_value)].reset_index(drop=True)
        return self
    
    def groupby(self, grouping_columns:List[str]|str, summing_column:List[str]|str):
        self._check_list_subset(grouping_columns, list(self.data.columns))
        self._check_list_subset(summing_column, list(self.data.columns))

        self.data = self.data.groupby(grouping_columns)[summing_column].sum().reset_index()
        return self


CRIME_RENAME_COLUMS = {
    'PSOS_MMW_Name':'ward_name_2022',
    'COUNCIL NAME':'council_name',
    'CALENDAR YEAR':'year',
    'CALENDAR MONTH':'month',
    'DETECTED CRIME':'count'
}
def main():
    base_url = 'https://www.scotland.police.uk'
    page_url = '/about-us/how-we-do-it/crime-data/'

    crime_urls = get_crime_data_url(base_url, page_url)
    crime_data = crime_data_scrapper(crime_urls)

    ward_boundaries_2007_path = ' /Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp'
    ward_2007_geometry, ward_code_2007_lookup = load_and_prepare_shapefile(ward_boundaries_2007_path, 'ONS_2010', 'Name', '2007', 27700, normalise_text)

    ward_boundaries_2022_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp'
    ward_2022_geometry, ward_code_2022_lookup = load_and_prepare_shapefile(ward_boundaries_2022_path, 'WD25CD', 'WD25NM', '2022', 27700, normalise_text)

    ward_2007_2022_map = calculate_overlap(ward_2007_geometry, ward_2022_geometry)
    ward_2007_2022_map = subset_columns(ward_2007_2022_map, ['ward_code_2007', 'ward_code_2022', 'overlap_pct'])

    ward_code_2007_lookup = apply_disambiguation(ward_code_2007_lookup, 'ward_code_2007', 'ward_name_2007', NAME_DISAMBIGUATION_2007)
    ward_code_2022_lookup = apply_disambiguation(ward_code_2022_lookup, 'ward_code_2022', 'ward_name_2022', NAME_DISAMBIGUATION_2022)

    ward_crime_list = []
    for _, value in crime_data.items():
        yearly_crime_data = CrimePipeline(value)
        yearly_crime_data = (
            yearly_crime_data.rename_cols(CRIME_RENAME_COLUMS)
            .subset_columns([col for col in CRIME_RENAME_COLUMS.values()])
            .filter_df('council_name', ['Western Isles - Eilean Siar', 'North Ayrshire'])
            .filter_df('ward_name_2022', ['.Other / Unknown', 'Other / Unknown'])
            .apply_manual_edits(col='ward_name_2022',mannual_edits=MANNUAL_WARD_NAME_EDITS)
            .normalise_column(normalise_func=normalise_text, col_to_normalise='ward_name_2022')
            .groupby(grouping_columns=['ward_name_2022', 'council_name', 'year', 'month'], summing_column='count')
            .left_join(data_to_merge=ward_code_2022_lookup, merging_column='ward_name_2022')
            .rename_cols({'ward_code_2022':'ward_code'})
            .drop_columns(['ward_name_2022', 'council_name'])
            .extract_df()
        )

        ward_crime_list.append(yearly_crime_data)
    crime_data = pd.concat(ward_crime_list, ignore_index=True)
    
    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'year',
        'month',
        'count',
    ]

    if DB_URL is not None:
        update_db(data=crime_data, db_url=DB_URL, table_name='ward_crime', required_columns=required_columns)

    return crime_data


if __name__ == '__main__':
    df = main()
