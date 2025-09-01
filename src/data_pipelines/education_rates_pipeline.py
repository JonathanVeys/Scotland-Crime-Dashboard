import os
from dotenv import load_dotenv      

import pandas as pd

from src.data_pipelines.preprocessing.spacial_processing import calculate_overlap, load_and_prepare_shapefile, apply_disambiguation
from src.data_pipelines.preprocessing.utils import normalise_text, rename_column_names, subset_columns
from src.data_pipelines.preprocessing.education_data_processing import interpolate_df, expand_date_range
from src.data_pipelines.DB.update_database import update_db
from src.data_pipelines.mapping import WARD_2007_COL_RENAME_DICT, MANNUAL_WARD_NAME_EDITS_2007, WARD_2022_COL_RENAME_DICT, MANNUAL_WARD_NAME_EDITS_2022, NAME_DISAMBIGUATION_2007, NAME_DISAMBIGUATION_2022

    

if __name__ == '__main__':
    ward_boundaries_2007_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp'
    ward_2007_geometry, ward_code_2007_lookup = load_and_prepare_shapefile(ward_boundaries_2007_path, 'ONS_2010', 'Name', '2007', 27700, normalise_text)

    ward_boundaries_2022_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp'
    ward_2022_geometry, ward_code_2022_lookup = load_and_prepare_shapefile(ward_boundaries_2022_path, 'WD25CD', 'WD25NM', '2022', 27700, normalise_text)

    ward_2007_2022_map = calculate_overlap(ward_2007_geometry, ward_2022_geometry)
    ward_2007_2022_map = ward_2007_2022_map[['ward_code_2007', 'ward_code_2022', 'overlap_pct']]

    ward_code_2007_lookup = apply_disambiguation(ward_code_2007_lookup, 'ward_code_2007', 'ward_name_2007', NAME_DISAMBIGUATION_2007)
    ward_code_2022_lookup = apply_disambiguation(ward_code_2022_lookup, 'ward_code_2022', 'ward_name_2022', NAME_DISAMBIGUATION_2022)



    education_data_2011 = pd.read_csv('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/education_data/education_ward_data_2011.csv', skiprows=12, skipfooter=5, engine='python')
    education_data_2011 = rename_column_names(education_data_2011, WARD_2007_COL_RENAME_DICT)               #Rename columns names
    education_data_2011['ward_name_2007'] = education_data_2011['ward_name_2007'].apply(normalise_text)     #Normalise the ward name column
    education_data_2011['ward_name_2007'] = education_data_2011['ward_name_2007'].map(                      #Apply mannual changes to certain ward names
        lambda x: MANNUAL_WARD_NAME_EDITS_2007.get(x,x)
    )

    #Select certain columns from data
    education_data_2011 = subset_columns(education_data_2011, list(WARD_2007_COL_RENAME_DICT.values()))
    #Merge data with ward code lookup to get ward codes
    education_data_2011 = education_data_2011.merge(ward_code_2007_lookup, on='ward_name_2007', how='left').drop('ward_name_2007', axis=1)

    #Pivot the data wider
    education_data_2011 = education_data_2011.pivot(
        columns='qualification',
        values='count',
        index='ward_code_2007'
    ).reset_index()

    education_data_2011 = education_data_2011.rename(
        columns={
            'ward_code_2007':'ward_code_2007',
            'All people aged 16 and over: Level 1':'qual_level_1_start',
            'All people aged 16 and over: Level 2':'qual_level_2_start',
            'All people aged 16 and over: Level 3':'qual_level_3_start',
            'All people aged 16 and over: Level 4 and above':'qual_level_4_start',
            'All people aged 16 and over: No qualifications':'no_qual_start',
            'All people aged 16 and over: Total':'total_population_start'
        }
    )

    cols = ['qual_level_1_start', 'qual_level_2_start', 'qual_level_3_start', 'qual_level_4_start', 'no_qual_start']
    education_data_2011[cols] = education_data_2011[cols].div(education_data_2011['total_population_start'], axis=0)
    education_data_2011 = education_data_2011.drop('total_population_start', axis=1)

    education_data_2011 = education_data_2011.merge(ward_2007_2022_map, on='ward_code_2007', how='left')
    education_data_2011[cols] = education_data_2011[cols].mul(education_data_2011['overlap_pct'], axis=0)
    education_data_2011 = education_data_2011.groupby(['ward_code_2022'])[cols].sum().reset_index()

    education_data_2011['date_start'] = pd.to_datetime('2011-01-01')



    
    education_data_2022 = pd.read_csv('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/education_data/eductaion_ward_data_2022.csv', skiprows=10, skipfooter=8, engine='python')
    education_data_2022 = rename_column_names(education_data_2022, WARD_2022_COL_RENAME_DICT)               #Rename columns names
    education_data_2022['ward_name_2022'] = education_data_2022['ward_name_2022'].apply(normalise_text)     #Normalise the ward name column
    education_data_2022['ward_name_2022'] = education_data_2022['ward_name_2022'].map(                      #Apply mannual changes to certain ward names
        lambda x: MANNUAL_WARD_NAME_EDITS_2022.get(x,x)
    )

    #Subset data
    education_data_2022 = subset_columns(education_data_2022, list(WARD_2022_COL_RENAME_DICT.values()))
    #Merge data with ward code lookup to get ward codes
    education_data_2022 = education_data_2022.merge(ward_code_2022_lookup, on='ward_name_2022', how='left').drop('ward_name_2022', axis=1)

    education_data_2022 = education_data_2022.pivot(
        columns='qualification',
        values='count',
        index='ward_code_2022'
    ).reset_index()

    education_data_2022 = education_data_2022.rename(
        columns={
            'ward_code_2022':'ward_code_2022',
            'All people aged 16 and over':'total_population_end',
            'No qualifications':'no_qual_end',
            'Lower school qualifications':'qual_level_1_end',
            'Upper school qualifications':'qual_level_2_end',
            'Further Education and sub-degree Higher Education qualifications incl. HNC/HNDs':'qual_level_3_end',
            'Apprenticeship qualifications':'qual_level_3.5_end',
            'Degree level qualifications or above':'qual_level_4_end'
        }
    )
    education_data_2022['qual_level_3_end'] = education_data_2022[['qual_level_3_end', 'qual_level_3.5_end']].sum(axis=1)
    education_data_2022 = education_data_2022.drop('qual_level_3.5_end', axis=1)

    cols = ['qual_level_1_end', 'qual_level_2_end', 'qual_level_3_end', 'qual_level_4_end', 'no_qual_end']
    education_data_2022[cols] = education_data_2022[cols].div(education_data_2022['total_population_end'], axis=0)
    education_data_2022 = education_data_2022.drop('total_population_end', axis=1)
    education_data_2022['date_end'] = pd.to_datetime('2022-01-01')


    education_data = pd.merge(education_data_2011, education_data_2022, on='ward_code_2022')    
    education_data = education_data.rename(columns={'ward_code_2022':'ward_code'})
    
    education_data_final_list = []
    for index, row in education_data.iterrows():
        expanded_row = expand_date_range(row)
        expanded_row = expanded_row[['ward_code', 'date'] + [col for col in expanded_row.columns if col not in ['ward_code', 'date']]]
        expanded_row = interpolate_df(expanded_row, ['qual_level_1', 'qual_level_2', 'qual_level_3', 'qual_level_4', 'no_qual'])
        education_data_final_list.append(expanded_row)

    education_data_final = pd.concat(education_data_final_list, ignore_index=True)


    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'date',
        'qual_level_1',
        'qual_level_2',
        'qual_level_3',
        'qual_level_4',
        'no_qual',
    ]

    if DB_URL is not None:
        update_db(data=education_data_final, db_url=DB_URL, table_name='education_ward_data', required_columns=required_columns)


