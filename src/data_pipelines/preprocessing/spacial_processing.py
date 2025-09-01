from typing import List, Callable, cast

import geopandas as gpd
import pandas as pd

def extract_lookup(gdf:gpd.GeoDataFrame, ward_name_col:str, ward_code_col:str):
    '''
    A function that extracts a lookup table between wards names and ward codes from a geojson file
    '''
    missing_columns = [col for col in [ward_code_col, ward_name_col] if col not in gdf.columns]
    if missing_columns:
        raise ValueError(f'Column ({missing_columns}) are not present in gdf columns')      

    return gdf[[ward_code_col, ward_name_col]].copy()

def extract_shapefile(gdf:gpd.GeoDataFrame, ward_code_col:str, geomtry_col = 'geometry'):
    '''
    A function that extracts the geomtery data from a geojson file
    '''
    if ward_code_col not in gdf.columns:
        raise ValueError(f'Columns ({ward_code_col}) not present in gdf columns')
    
    if geomtry_col not in gdf.columns:
        print('Warning: Could not find "geomtry" column, attempting automatic detection')

        geomtry_col = [col for col in gdf.columns if gdf[col].dtype.name == "geometry"]

        if len(geomtry_col) != 1:
            raise ValueError('Failed to find column with geomtry, please ensure geometry column is labelled "geometry"')
    
    return gdf[[ward_code_col, geomtry_col]].copy()

def extract_lookup_and_shapefile(gdf:gpd.GeoDataFrame, ward_code_col:str, ward_name_col:str, normalise_func:Callable[[str],str]|None = None):
    '''
    A function that uses a shapefile to extract a lookup table for ward names to ward codes, and a shapefile that encodes each wards shape
    '''
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError(f'Expected GeoDataFrame, got {type(gpd).__name__}')

    gdf_lookup = extract_lookup(gdf, ward_name_col, ward_code_col)
    gdf_shapefile = extract_shapefile(gdf, ward_code_col)

    if normalise_func is not None:
        gdf_lookup[ward_name_col] = gdf_lookup[ward_name_col].apply(normalise_func)

    return gdf_shapefile, gdf_lookup

def prepare_wards(gdf, ID:str, crs_epsg:int) -> gpd.GeoDataFrame:
    '''
    
    '''
    if not isinstance(gdf, gpd.GeoDataFrame):
        try:
            gdf = gpd.GeoDataFrame(gdf)
        except:
            raise TypeError(f'Expected GeoDataFrame, instead got {type(gdf).__name__}.')
    
    if 'geometry' in gdf.columns:
        gdf = cast(gpd.GeoDataFrame, gdf.set_geometry('geometry'))
    else:
        raise ValueError("Failed to find 'geometry' column")

    gdf.to_crs(epsg=crs_epsg, inplace=True)
    gdf[f'area_{ID}'] = gdf.geometry.area

    return gdf

def calculate_overlap(df_1:gpd.GeoDataFrame, df_2:gpd.GeoDataFrame):
    '''
    
    '''
    df = gpd.overlay(df_1, df_2, how='intersection', keep_geom_type=True)
    df['overlap_area'] = df.geometry.area
    df['overlap_pct'] = df['overlap_area'] / df[[c for c in df.columns if "area" in c.lower()][0]]

    return df

def load_and_prepare_shapefile(path:str, code_col:str, name_col:str, year_id:str, crs_epsg:int, normalise_func:Callable|None = None):
    '''

    '''
    gdf = gpd.read_file(path, encoding='latin1')
    gdf = gdf[gdf[code_col].str.startswith('S')].reset_index(drop=True)
    gdf.rename(
        columns = {
            code_col:f'ward_code_{year_id}',
            name_col:f'ward_name_{year_id}'
        },
        inplace = True
    )

    geom, lookup = extract_lookup_and_shapefile(gdf, f'ward_code_{year_id}', f'ward_name_{year_id}', normalise_func)
    geom = prepare_wards(geom, year_id, crs_epsg)

    return geom, lookup

def apply_disambiguation(df, code_col: str, name_col: str, disambiguation: dict) -> pd.DataFrame:
    '''
    
    '''

    for key, value in disambiguation.items():
        key_idx = list(df.index[df[code_col] == key])
        df.loc[key_idx, name_col] = value
        

    return df
