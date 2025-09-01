import os
from dotenv import load_dotenv      

import pandas as pd

from src.data_pipelines.preprocessing.spacial_processing import load_and_prepare_shapefile
from src.data_pipelines.preprocessing.utils import rename_column_names
from src.data_pipelines.DB.update_database import update_db


if __name__ == '__main__':
    ward_boundaries_2022_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp'
    _, ward_code_lookup = load_and_prepare_shapefile(ward_boundaries_2022_path, 'WD25CD', 'WD25NM', '2022', 27700)
    ward_code_lookup = pd.DataFrame(ward_code_lookup)
    ward_code_lookup = rename_column_names(ward_code_lookup, {'ward_code_2022':'ward_code', 
                                                              'ward_name_2022':'ward_name'})
    
    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'ward_name'
    ]

    if DB_URL is not None:
        update_db(data=ward_code_lookup, db_url=DB_URL, table_name='ward_code_name', required_columns=required_columns)
