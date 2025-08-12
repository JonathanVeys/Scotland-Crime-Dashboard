import pandas as pd
import geopandas as gpd

from scraping.crime_scrapper import get_crime_data_url, crime_data_scrapper
from preprocessing.utils import normalise_text, subset_columns, rename_column_names, split_dataset
from preprocessing.spacial_processing import load_and_filter_wards, calculate_overlap, prepare_wards


if __name__ == '__main__':

    def crime_data_processing(df:pd.DataFrame) -> pd.DataFrame:
        df = subset_columns(df, columns_to_keep = ['PSOS_MMW_Name', 'COUNCIL NAME', 'CALENDAR YEAR', 'CALENDAR MONTH', 'CRIME GROUP', 'GROUP DESCRIPTION', 'DETECTED CRIME'])

        rename_dict = {
            'PSOS_MMW_Name':'ward_name', 
            'COUNCIL NAME':'council_name', 
            'CALENDAR YEAR':'year', 
            'CALENDAR MONTH':'month', 
            'CRIME GROUP':'crime_group', 
            'GROUP DESCRIPTION':'group_description', 
            'DETECTED CRIME':'crime_count'
            }
        
        df = rename_column_names(df, rename_dict)

        df = df[df['ward_name'] != '.Other / Unknown']
        df = df[df['ward_name'] != 'Other / Unknown']
        return df

    base_url = 'https://www.scotland.police.uk'
    page_url = '/about-us/how-we-do-it/crime-data/'

    crime_urls = get_crime_data_url(base_url, page_url)
    crime_data = crime_data_scrapper(crime_urls)

    # for key in crime_data.keys():
    #     df = crime_data[key]
    #     df = crime_data_processing(df)
    #     crime_data[key] = df

    ward_code_name_lookup = pd.read_csv('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/ward_names_lookup.csv', encoding='latin1')
    ward_code_name_lookup = ward_code_name_lookup[['MMWard_Code', 'MMWard_Name', 'LA_Name']].drop_duplicates().reset_index(drop=True)
    ward_code_name_lookup['ward_name_normalised'] = ward_code_name_lookup['MMWard_Name'].apply(normalise_text)

    merge_map = {
        'Stevenston':'Saltcoats and Stevenston',
        'Saltcoats':'Saltcoats and Stevenston',

        'North Coast and Cumbraes':'North Coast',
        'Dalry and West Kilbride':'North Coast',
        'Kilbirnie and Beith':'North Coast'
    }

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
        "Bo'ness and Blackness":"Boness and Blackness",
        "Barraigh Bhatarsaigh Eirisgeigh Uibhist Deas":"Barraigh, Bhatarsaigh, Eirisgeigh agus Uibhist a Deas"
    }

    for key in crime_data.keys():
        df = crime_data[key]

        #Data processing before lookup merge
        df = crime_data_processing(df)
        df['ward_name'] = df['ward_name'].replace(merge_map)
        df['ward_name'] = df['ward_name'].fillna(df['ward_name'])
        df.replace(manual_names_edit_crime_data, inplace=True)

        #Give north east and north isles unique names since there are multiple of these wards in the data
        ward_code_name_lookup.loc[ward_code_name_lookup['MMWard_Name'] == 'North East', 'MMWard_Name'] = (ward_code_name_lookup['MMWard_Name'] + ' - ' + ward_code_name_lookup['LA_Name'])
        ward_code_name_lookup.loc[ward_code_name_lookup['MMWard_Name'] == 'North Isles', 'MMWard_Name'] = (ward_code_name_lookup['MMWard_Name'] + ' - ' + ward_code_name_lookup['LA_Name'])

        #Use the normalise function on the ward column in data and lookup to get rid of white spaces, special characters ...
        df['ward_name_normalised'] = df['ward_name'].apply(normalise_text)
        ward_code_name_lookup['ward_name_normalised'] = ward_code_name_lookup['MMWard_Name'].apply(normalise_text)

        hebredies_crime_data, rest_scotland_crime_data = split_dataset(df, 'Western Isles - Eilean Siar', 'council_name')

        df = df.merge(ward_code_name_lookup, on='ward_name_normalised', how='left')

        wards_2017_path = '/Users/jonathancrocker/Downloads/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp'
        wards_2017 = gpd.read_file(wards_2017_path)
        wards_2017 = wards_2017[wards_2017['Council'] == 'Na h-Eileanan an Iar']
        wards_2017 = prepare_wards(wards_2017, '2017', 'Name', normalise_text, 27700)

        wards_2022_path = '/Users/jonathancrocker/Downloads/Wards_(May_2025)_Boundaries_UK_BFC_(V2)/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp'
        wards_2022 = gpd.read_file(wards_2022_path)
        hebrides_2022_wards_codes = ['S13002608', 'S13003138', 'S13003136', 'S13003140', 'S13003142', 'S13003141', 'S13003139', 'S13003137', 'S13003144', 'S13003143', 'S13003135']
        wards_2022 = wards_2022[wards_2022['WD25CD'].isin(hebrides_2022_wards_codes)]
        wards_2022 = prepare_wards(wards_2022, '2022', 'WD25NM', normalise_text, 27700)

        ward_2017_2022_map = calculate_overlap(wards_2017, wards_2022)
        ward_2017_2022_map = subset_columns(ward_2017_2022_map, ['WD25NM', 'overlap_pct', 'ward_name_normalised'])

        hebredies_crime_data = hebredies_crime_data.merge(ward_2017_2022_map, on='ward_name_normalised', how='left')
        hebredies_crime_data = (
            hebredies_crime_data
            .groupby(['WD25NM', 'council_name', 'year', 'month', 'crime_group', 'group_description'])['crime_count']
            .sum()
            .reset_index()
            .sort_values(by=['WD25NM', 'year', 'month'])
            .rename(columns={'WD25NM':'ward_name'})
        )
        hebredies_crime_data['ward_name_normalised'] = hebredies_crime_data['ward_name'].apply(normalise_text)

        df = pd.concat([hebredies_crime_data, rest_scotland_crime_data])

        df = df.merge(ward_code_name_lookup, on='ward_name_normalised', how='left')

        # print(df)

        missing_codes = df[['ward_name', 'council_name', 'ward_name_normalised', 'MMWard_Name', 'MMWard_Code']]
        missing_codes = missing_codes[pd.isna(missing_codes['MMWard_Code'])].drop_duplicates().reset_index(drop=True)
        print(missing_codes)

        