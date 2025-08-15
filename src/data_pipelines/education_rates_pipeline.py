import geopandas as gpd
import pandas as pd


from src.data_pipelines.preprocessing.spacial_processing import prepare_wards, calculate_overlap
from src.data_pipelines.preprocessing.utils import normalise_column_name, normalise_text


ward_boundaries_2007 = gpd.read_file('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp')
ward_boundaries_2007 = ward_boundaries_2007[['Name', 'geometry']]
ward_boundaries_2007 = prepare_wards(ward_boundaries_2007, '2007', 'Name', 'test', 27700)

ward_boundaries_2022 = gpd.read_file('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp')
ward_boundaries_2022 = ward_boundaries_2022[ward_boundaries_2022['WD25CD'].str.startswith('S')]
ward_boundaries_2022 = ward_boundaries_2022[['WD25NM', 'geometry']]
ward_boundaries_2022 = prepare_wards(ward_boundaries_2022, '2022', 'WD25NM', 'test', 27700)

ward_2007_2022_map = calculate_overlap(ward_boundaries_2007, ward_boundaries_2022)
ward_2007_2022_map['ward_name_normalised'] = ward_2007_2022_map['Name'].apply(normalise_text)
ward_2007_2022_map = ward_2007_2022_map[['Name', 'WD25NM', 'overlap_pct', 'ward_name_normalised']]


education_data_2011 = pd.read_csv('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/education_data/education_ward_data_2011.csv', skiprows=12, skipfooter=5)

mannual_ward_edits = {
    "Eilean a' Ch?o":"Eilean á Chèo",
    "Inverness Millburn ":"Inverness Milburn",
    "Land Caithness Ward":"Landward Caithness",
    "Monifieth and Sidlaw ":"Monifeith and Sidlaw"
}

education_data_2011 = education_data_2011[['Electoral Ward 2007', 'Highest level of qualification', 'Count']]
education_data_2011 = education_data_2011.pivot(
    index='Electoral Ward 2007',
    columns='Highest level of qualification',
    values='Count'
).reset_index()

column_rename_dict = {col:normalise_column_name(col) for col in education_data_2011.columns}
education_data_2011.rename(columns=column_rename_dict, inplace=True)
education_data_2011['electoral_ward_2007'] = education_data_2011['electoral_ward_2007'].replace(mannual_ward_edits)
education_data_2011['ward_name_normalised'] = education_data_2011['electoral_ward_2007'].apply(normalise_text)


education_data_2011 = education_data_2011.merge(ward_2007_2022_map, on='ward_name_normalised', how='left')
data_cols = ['all_people_aged_16_and_over:level_1', 'all_people_aged_16_and_over:level_2', 'all_people_aged_16_and_over:level_3', 'all_people_aged_16_and_over:level_4_and_above', 'all_people_aged_16_and_over:no_qualifications', 'all_people_aged_16_and_over:total']

education_data_2011[data_cols] = education_data_2011[data_cols].mul(education_data_2011['overlap_pct'], axis=0)
education_data_2011 = education_data_2011.groupby(['WD25NM'])[data_cols].sum()

mannual_rename = {
    'all_people_aged_16_and_over:total':'total_population',
    'all_people_aged_16_and_over:level_1':'lower_school',
    'all_people_aged_16_and_over:level_2':'upper_school',
    'all_people_aged_16_and_over:level_3':'further_education',
    'all_people_aged_16_and_over:level_4_and_above':'degree_or_above',
    'all_people_aged_16_and_over:no_qualifications':'no_qualification'
}
education_data_2011 = education_data_2011.rename(columns=mannual_rename).reset_index()
education_data_2011 = education_data_2011[['WD25NM', 'total_population', 'lower_school', 'upper_school', 'further_education', 'degree_or_above', 'no_qualification']]
print(education_data_2011)


