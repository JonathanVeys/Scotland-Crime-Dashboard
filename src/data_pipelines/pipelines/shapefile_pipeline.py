from pathlib import Path
import json
from typing import List
from dotenv import load_dotenv
import os

import pandas as pd
import geopandas as gpd

from src.data_pipelines.preprocessing.base_pipeline import BasePipeline
from src.DB.DatabaseClient import DatabaseWriter

class BoundaryPipeline(BasePipeline):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)

    @classmethod
    def from_path(cls, path:str):
        if not Path(path).exists:
            raise ValueError('Path does not point to a file')
        data = gpd.read_file(path)
        return cls(data)
    
    def filter_df(self, filter_col:str, filter_values:List[str]|str):
        self._check_list_subset(filter_col, list(self.data.columns))

        if isinstance(filter_values, str):
            filter_values = [filter_values]

        for filter_value in filter_values:    
            self.data = self.data[self.data[filter_col].str.contains(filter_value, case=False)].reset_index(drop=True)
        return self
    
    def change_crs(self, crs:int):
        self.data.to_crs(epsg=crs, inplace=True)  #type: ignore
        return self 
    
    def extract_gdf(self):
        return gpd.GeoDataFrame(self.data)


if __name__ == '__main__':
    CURRENT_DIR = Path(__file__).resolve()
    for parent in CURRENT_DIR.parents:
        if (parent / "data").exists():
            PACKAGE_DIR = parent
            break
    else:
        raise FileNotFoundError("Could not find project root containing 'data' folder")
    
    with open(PACKAGE_DIR/'src/data_pipelines/pipelines/config/transformations.json', 'r') as f:
        config = json.load(f)

    boundary_data = BoundaryPipeline.from_path(PACKAGE_DIR/config['ward_boundaries_2022']['path'])
    boundary_data = (
        boundary_data.subset_columns(['WD25CD', 'geometry'])
        .filter_df('WD25CD', 'S')
        .rename_cols({'WD25CD':'ward_code'})
        .change_crs(4326)
        .extract_gdf()
    )

    boundary_data["geometry"] = boundary_data["geometry"].simplify(tolerance=0.002, preserve_topology=True)

    
    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")
    databaseClient = DatabaseWriter(DB_URL=DB_URL)
    databaseClient.update_from_gpd(boundary_data, 'ward_boundary_data')

