import geopandas as gpd
from pandas import DataFrame

from src.data_pipelines.preprocessing.base_pipeline import BasePipeline


class BoundaryPipeline(BasePipeline):
    def __init__(self, data: DataFrame):
        super().__init__(data)

    @classmethod
    def from_file(cls, path:str):
        try:
            data = gpd.read_file(path)
        except:
            raise TypeError(f'Failed to load data from {path}')
        return cls(data)
    
    def starts_with(self, col_to_filter:str, filter_value:str):
        self.data = self.data[self.data[col_to_filter].str.startswith(filter_value)]
        return self
    

ward_boundaries_2007_path = ' /Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp'
ward_boundaries_2007_data = BoundaryPipeline.from_file(ward_boundaries_2007_path)
ward_boundaries_2007_data = (
    ward_boundaries_2007_data.starts_with('ONS_2010', 'S')
)
print(ward_boundaries_2007_data.data)