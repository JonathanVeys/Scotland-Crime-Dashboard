import json
from pathlib import Path
from typing import Callable

import geopandas as gpd
from pandas import DataFrame

from src.data_pipelines.preprocessing.base_pipeline import BasePipeline
from src.data_pipelines.preprocessing.utils import normalise_text


class BoundaryPipeline(BasePipeline):
    def __init__(self, data: DataFrame):
        super().__init__(data)

    @classmethod
    def from_file(cls, path:str):
        '''
        
        '''
        try:
            data = gpd.read_file(path)
        except:
            raise TypeError(f'Failed to load data from {path}')
        return cls(data)
    
    def starts_with(self, col_to_filter:str, filter_value:str):
        '''
        
        '''
        self.data = self.data[self.data[col_to_filter].str.startswith(filter_value)]
        return self
    
    def find_col(self, search_string):
        col_matches = [col for col in self.data.columns if search_string in col]

        if len(col_matches) == 1:
            return col_matches[0]
        elif len(col_matches) == 0:
            raise ValueError(f'Error: No column found containing string f{search_string}')
        else:
            raise ValueError(f'Multiple matches found: {col_matches}')
    
    def extract_shapefile(self, geometry_col:str='geometry', ward_code_col:str|None = None):
        '''
        
        '''
        self._check_list_subset(geometry_col, list(self.data.columns))
        if ward_code_col is None:
            ward_code_col_list = [col for col in self.data.columns if 'ward_code' in col]
            if len(ward_code_col_list) == 1:
                ward_code_col = ward_code_col_list[0]
            else:
                raise ValueError('Failed to find ward code, please select manually')
            
        self.shapefile = self.data[[ward_code_col, geometry_col]]
        
        return self
    
    def extract_lookup(self, ward_code_col:str|None=None, ward_name_col:str|None=None, normalise_func:None|Callable=None):
        '''
        
        '''
        self._check_list_subset([col for col in [ward_code_col, ward_name_col] if col is not None], str(self.data.columns))
        if ward_code_col is None:
            ward_code_col = self.find_col('ward_code')
        if ward_name_col is None:
            ward_name_col = self.find_col('ward_name')

        self.lookup = self.data[[ward_name_col, ward_code_col]]

        if normalise_func:
            self.lookup = self.lookup[ward_name_col].apply(normalise_func)
        return self



def main(): 
    CURRENT_DIR = Path(__file__).resolve().parent
    PACKAGE_DIR = CURRENT_DIR.parent.parent.parent

    with open(PACKAGE_DIR / 'src/data_pipelines/pipelines/config/transformations.json') as f:
        config = json.load(f)
    ward_boundaries_2007_config = config['ward_boundaries_2007']


    ward_boundaries_2007_path = ' /Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp'
    ward_boundaries_2007_data = BoundaryPipeline.from_file(ward_boundaries_2007_path)
    ward_boundaries_2007_data = (ward_boundaries_2007_data
        .starts_with('ONS_2010', 'S')
        .rename_cols(ward_boundaries_2007_config['transformations']['column_rename'])
        .extract_shapefile()
        .extract_lookup(normalise_func=normalise_text)
    )
    print(ward_boundaries_2007_data.shapefile)
    print(ward_boundaries_2007_data.lookup)
    print(ward_boundaries_2007_data.data)


if __name__ == '__main__':
    main()