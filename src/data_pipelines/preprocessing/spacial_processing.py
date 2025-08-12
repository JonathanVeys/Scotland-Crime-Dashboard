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
    df['overlap_pct'] = df['overlap_area'] / df['area_2017']
    df['ward_name_normalised'] = df['Name'].apply(normalise_text)

    return df



# wards_2022 = gpd.read_file('/Users/jonathancrocker/Downloads/Wards_(May_2025)_Boundaries_UK_BFC_(V2)/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp')
# wards_2022 = wards_2022[wards_2022['WD25CD'].str.startswith('S')].reset_index(drop=True)
# hebrides_2022_wards_codes = ['S13002608', 'S13003138', 'S13003136', 'S13003140', 'S13003142', 'S13003141', 'S13003139', 'S13003137', 'S13003144', 'S13003143', 'S13003135']
# hebrides_2022_wards = wards_2022[wards_2022['WD25CD'].isin(hebrides_2022_wards_codes)]
# hebrides_2022_wards = hebrides_2022_wards[['WD25CD', 'WD25NM', 'geometry']].reset_index(drop=True)   
# hebrides_2022_wards['WD25NM_processed'] = hebrides_2022_wards['WD25NM'].apply(normalise_text)
# hebrides_2022_wards = hebrides_2022_wards.to_crs(epsg=27700)
# hebrides_2022_wards['area_2022'] = hebrides_2022_wards.geometry.area


# wards_2017 = gpd.read_file('/Users/jonathancrocker/Downloads/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp')
# hebrides_2017_wards = wards_2017[wards_2017['Council'] == 'Na h-Eileanan an Iar']
# hebrides_2017_wards = hebrides_2017_wards[['Name', 'geometry']].reset_index(drop=True)
# hebrides_2017_wards['area_2017'] = hebrides_2017_wards.geometry.area
# hebrides_2017_wards['Name_processed'] = hebrides_2017_wards['Name'].apply(normalise_text)
# hebrides_2017_wards = hebrides_2017_wards.to_crs(epsg=27700)
# hebrides_2017_wards['area_2017'] = hebrides_2017_wards.geometry.area


# map_2017_2022_wards = gpd.overlay(hebrides_2017_wards, hebrides_2022_wards, how='intersection')
# map_2017_2022_wards['overlap_area'] = map_2017_2022_wards.geometry.area
# map_2017_2022_wards['overlap_pct'] = map_2017_2022_wards['overlap_area'] / map_2017_2022_wards['area_2017']
# map_2017_2022_wards.rename(columns={'WD25NM':'ward_name_2022', 'WD25NM_processed':'ward_name_2022_processed', 'Name':'ward_name', 'Name_processed':'ward_name_normalised', 'WD25CD':'ward_code'}, inplace=True)
# map_2017_2022_wards = map_2017_2022_wards[['ward_name_2022', 'ward_name', 'ward_name_2022_processed','ward_name_normalised', 'ward_code', 'overlap_pct']]