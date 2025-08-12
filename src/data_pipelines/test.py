import geopandas as gpd
from scraping.crime_scrapper import get_crime_data_url, crime_data_scrapper
from preprocessing.crime_data_processing import subset_columns, rename_column_names
from preprocessing.utils import normalise_text

import pandas as pd


manual_names_edit_crime_data = {
        "Ft William and Ardnamurchan":"Fort William and Ardnamurchan",
        "Eilean a Cheo":"Eilean a' Chèo",
        "Lerwick North":"Lerwick North and Bressay",
        "Glenboig and Moodiesburn":"Gartcosh, Glenboig and Moodiesburn",
        "North East - Glasgow":"North East -  Glasgow City",
        "North East - Tayside":"North East - Dundee City",
        "North Isles - Orkney":"North Isles - Orkney Islands",
        "North Isles - Shetland":"North Isles - Shetland Islands",
        "Steornabhagh a Tuath":"Steòrnabhagh a Tuath",
        "Steornabhagh a Deas":"Steòrnabhagh a Deas",
        "Bo'ness and Blackness":"Boness and Blackness",
        "Ardrossan and Arran":"Ardrossan",
        "Sgire an Rubha":"Sgìre an Rubha",
        # "Bo'ness and Blackness":"Boíness and Blackness"
    }

def get_2022_ward_data():
    wards_2022 = gpd.read_file('/Users/jonathancrocker/Downloads/Wards_(May_2025)_Boundaries_UK_BFC_(V2)/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp')

    wards_2022 = wards_2022[['WD25CD', 'WD25NM', 'geometry']]
    wards_2022.rename(columns = {'WD25CD':'ward_code_2022', 'WD25NM':'ward_name_2022'}, inplace=True)

    wards_2022 = wards_2022[wards_2022['ward_code_2022'].str.startswith('S')].reset_index(drop=True)
    wards_2022['area_2022'] = wards_2022.geometry.area

    wards_2022.loc[wards_2022['ward_code_2022'] == 'S13002830', 'ward_name_2022'] = 'North East - Dundee'
    wards_2022.loc[wards_2022['ward_code_2022'] == 'S13003133', 'ward_name_2022'] = 'North East - Glasgow'
    wards_2022.loc[wards_2022['ward_code_2022'] == 'S13002737', 'ward_name_2022'] = 'North Isles - Orkney'
    wards_2022.loc[wards_2022['ward_code_2022'] == 'S13002772', 'ward_name_2022'] = 'North Isles - Shetland'

    wards_2022['ward_name_2022'] = wards_2022['ward_name_2022'].apply(normalise_text)

    return wards_2022

def get_2017_ward_data():
    wards_2017 = gpd.read_file('/Users/jonathancrocker/Downloads/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp')
    wards_2017['Name'] = wards_2017['Name'].apply(normalise_text)

    # wards_2017 = wards_2017[['WD22CD', 'WD22NM', 'geometry']]
    # wards_2017.rename(columns={'WD22CD':'ward_code_2017', 'WD22NM':'ward_name_2017'}, inplace=True)

    # wards_2017 = wards_2017[wards_2017['ward_code_2017'].str.startswith('S')].reset_index(drop=True)
    # wards_2017['area_2017'] = wards_2017.geometry.area

    # wards_2017 = wards_2017[['ONS_2010', 'Name', 'geometry']]
    # wards_2017.rename(columns = {'ONS_2010':'ward_code_2017', 'Name':'ward_name_2017'}, inplace = True)
    # wards_2017['area_2017'] = wards_2017.geometry.area

    # wards_2017.loc[wards_2017['ward_code_2017'] == 'S13002550', 'ward_name_2017'] = 'North East - Dundee'
    # wards_2017.loc[wards_2017['ward_code_2017'] == 'S13002661', 'ward_name_2017'] = 'North East - Glasgow'

    return wards_2017

def get_crime_data():
    base_url = 'https://www.scotland.police.uk'
    page_url = '/about-us/how-we-do-it/crime-data/'

    crime_urls = get_crime_data_url(base_url, page_url)

    crime_data = crime_data_scrapper(crime_urls)

    for key in crime_data.keys():
        df = crime_data[key]
        df = subset_columns(df, columns_to_keep = ['PSOS_MMW_Name', 'COUNCIL NAME', 'CALENDAR YEAR', 'CALENDAR MONTH', 'CRIME GROUP', 'GROUP DESCRIPTION', 'DETECTED CRIME'])

        rename_dict = {
            'PSOS_MMW_Name':'ward_name_2017', 
            'COUNCIL NAME':'council_name', 
            'CALENDAR YEAR':'year', 
            'CALENDAR MONTH':'month', 
            'CRIME GROUP':'crime_group', 
            'GROUP DESCRIPTION':'group_description', 
            'DETECTED CRIME':'crime_count'
            }
        
        df = rename_column_names(df, rename_dict)
        df = df[df['ward_name_2017'] != '.Other / Unknown'].reset_index(drop=True)
        df.replace(manual_names_edit_crime_data, inplace=True)
        df['ward_name_2017'] = df['ward_name_2017'].apply(normalise_text)

        crime_data[key] = df

    # crime_data = pd.concat(crime_data).reset_index(drop=True)
    
    return crime_data


if __name__ == '__main__':
    wards_2017 = get_2017_ward_data()
    wards_2022 = get_2022_ward_data()
    crime_data = get_crime_data()['MMW Detected 2024 25']

    # print(wards_2017)
    # print(wards_2022)

    crime_data_wards = crime_data['ward_name_2017'].unique()
    wards_2017_names = wards_2017['Name'].unique()
    wards_2022_names = wards_2022['ward_name_2022'].unique()
    combined_wards = [*wards_2017_names, *wards_2022_names]

    print(len(wards_2017_names))
    print(len(wards_2022_names))
    print(len(combined_wards))

    wards_2017_diff = [ward for ward in crime_data_wards if ward not in wards_2017_names]
    wards_2022_diff = [ward for ward in crime_data_wards if ward not in wards_2022_names]
    combined_diff = [ward for ward in crime_data_wards if ward not in combined_wards]

    print(len(wards_2017_diff))
    print(len(wards_2022_diff))
    print(len(combined_diff))
    print(combined_diff)
    # print(combined_wards)
    # print(len(combined_wards))

    # print(wards_2017[wards_2017['Name'].str.contains('Arran', case=False, na=False)])
    # print(crime_data[crime_data['ward_name_2017'].str.contains('Caithness', case=False, na=False)])

    # wards_2017_set = set(wards_2017['Name'].unique())
    # wards_crime_set = set(crime_data['ward_name_2017'].unique())

    # print(wards_crime_set.difference(wards_2017_set))


    # print(wards_2017[wards_2017['ward_name_2017'].str.contains('Ardrossan and Arran', case=False, na=False)])
    # print(wards_2022[wards_2022['ward_name_2022'].str.contains('Arran', case=False, na=False)])
    # print(crime_data['ward_name_2017'].unique())

    # print(wards_2022[wards_2022['ward_name_2022'] == 'Wick and East Caithness'])

    # crime_data = get_crime_data()
    # crime_data = crime_data['MMW Detected 2024 25']
    # # crime_data.to_csv('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/crime_data.csv')

    # wards_2017 = wards_2017.to_crs(wards_2022.crs)
    # intersection = gpd.overlay(wards_2022, wards_2017, how='intersection', keep_geom_type=True)
    # intersection['area_overlap'] = intersection.geometry.area

    # intersection['overlap_pct'] = intersection['area_overlap'] / intersection['area_2017']
    # # intersection.to_csv('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/intersection.csv')

    # crime_data_processed = crime_data.merge(intersection[['ward_code_2017', 'ward_name_2017', 'ward_code_2022', 'ward_name_2022', 'overlap_pct']], on='ward_name_2017', how='left')
    # crime_data_processed = crime_data_processed.sort_values(by='overlap_pct').reset_index(drop=True)

    # missing_links = crime_data_processed[['ward_name_2017', 'council_name', 'ward_code_2017', 'ward_code_2022', 'ward_name_2022']]
    # missing_links = missing_links[pd.isna(missing_links['ward_name_2022'])].drop_duplicates().reset_index(drop=True)
    # print(crime_data_processed.sort_values(by='overlap_pct'))





# wards_2017 = wards_2017.to_crs(wards_2022.crs)

# intersection = gpd.overlay(wards_2017, wards_2022, how='intersection')

# intersection['area_overlap'] = intersection.geometry.area

# wards_2017['area_2017'] = wards_2017.geometry.area
# intersection = intersection.merge(
#     wards_2017[['CODE', 'ONS_2010', 'Name', 'area_2017']],
#     on='CODE',
#     how='left'
# )

# intersection['overlap_pct'] = intersection['area_overlap'] / intersection['area_2017']





# base_url = 'https://www.scotland.police.uk'
# page_url = '/about-us/how-we-do-it/crime-data/'

# crime_urls = get_crime_data_url(base_url, page_url)

# crime_data = crime_data_scrapper(crime_urls)

# for key in crime_data.keys():
#     df = crime_data[key]
#     df = subset_columns(df, columns_to_keep = ['PSOS_MMW_Name', 'COUNCIL NAME', 'CALENDAR YEAR', 'CALENDAR MONTH', 'CRIME GROUP', 'GROUP DESCRIPTION', 'DETECTED CRIME'])

#     rename_dict = {
#         'PSOS_MMW_Name':'ward_name', 
#         'COUNCIL NAME':'council_name', 
#         'CALENDAR YEAR':'year', 
#         'CALENDAR MONTH':'month', 
#         'CRIME GROUP':'crime_group', 
#         'GROUP DESCRIPTION':'group_description', 
#         'DETECTED CRIME':'crime_count'
#         }
    
#     df = rename_column_names(df, rename_dict)

#     crime_data[key] = df

# crime_data = pd.concat(crime_data).reset_index(drop=True)
# crime_data = crime_data[crime_data['ward_name'] != '.Other / Unknown'].reset_index(drop=True)

