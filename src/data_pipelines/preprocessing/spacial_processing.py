from typing import List

import geopandas as gpd
from .utils import normalise_text, subset_columns


def load_and_filter_wards(path:str, filter_col:str, filter_value:List[str]):
    wards = gpd.read_file(path)
    wards = wards[wards[filter_col].str.startswith(filter_value)].reset_index(drop=True)

    return wards

def prepare_wards(df:gpd.GeoDataFrame, ID:str, name_col:str, normalise_function, crs_epsg:int) -> gpd.GeoDataFrame:
    df = df[[name_col, 'geometry']].reset_index(drop=True)
    # df['ward_name_processed'] = df[name_col].apply(normalise_function)
    df = df.to_crs(epsg=crs_epsg)
    df[f'area_{ID}'] = df.geometry.area

    return df

def calculate_overlap(df_1:gpd.GeoDataFrame, df_2:gpd.GeoDataFrame):
    df = gpd.overlay(df_1, df_2, how='intersection')
    df['overlap_area'] = df.geometry.area
    df['overlap_pct'] = df['overlap_area'] / df[[c for c in df.columns if "area" in c.lower()][0]]
    # df['ward_name_normalised'] = df['Name'].apply(normalise_text)

    return df


