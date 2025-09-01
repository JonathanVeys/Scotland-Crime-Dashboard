import pandas as pd

from src.data_pipelines.preprocessing.spacial_processing import calculate_overlap, load_and_prepare_shapefile, apply_disambiguation
from src.data_pipelines.preprocessing.utils import normalise_text, rename_column_names, subset_columns
from src.data_pipelines.employment_mapping import NAME_DISAMBIGUATION_2007, NAME_DISAMBIGUATION_2022, MANNUAL_WARD_NAME_EDITS_2007

def main():
    ward_boundaries_2007_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/4th_Review_2007_2017_All_Scotland_wards/All_Scotland_wards_4th.shp'
    ward_2007_geometry, ward_code_2007_lookup = load_and_prepare_shapefile(ward_boundaries_2007_path, 'ONS_2010', 'Name', '2007', 27700, normalise_text)

    ward_boundaries_2022_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp'
    ward_2022_geometry, ward_code_2022_lookup = load_and_prepare_shapefile(ward_boundaries_2022_path, 'WD25CD', 'WD25NM', '2022', 27700, normalise_text)

    ward_2007_2022_map = calculate_overlap(ward_2007_geometry, ward_2022_geometry)
    ward_2007_2022_map = ward_2007_2022_map[['ward_code_2007', 'ward_code_2022', 'overlap_pct']]

    ward_code_2007_lookup = apply_disambiguation(ward_code_2007_lookup, 'ward_code_2007', 'ward_name_2007', NAME_DISAMBIGUATION_2007)
    ward_code_2022_lookup = apply_disambiguation(ward_code_2022_lookup, 'ward_code_2022', 'ward_name_2022', NAME_DISAMBIGUATION_2022)




    employment_2011_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/employment_data/Census_Employment_2011.csv'
    employment_2011 = pd.read_csv(employment_2011_path, skiprows=12, skipfooter=3)
    
    employment_cols = ['Economically active: Employee: Full-time', 'Economically active: Self-employed', 'Economically active: Unemployed']
    unemployment_cols = ['Economically active: Unemployed']

    employment_2011['unemployment_pct'] = employment_2011[unemployment_cols].sum(axis=1) / employment_2011[employment_cols].sum(axis=1)

    COLUMN_RENAME_DICT = {
            'Economic activity then Unemployed people aged 16 to 74 then Unemployed people aged 16 to 74, Never worked and long-term unemployed':'ward_name_2007',
            'All people aged 16 to 74':'total_pop',
            'Economically inactive: Looking after home or family':'caring_for_family',
            'Economically inactive: Long-term sick or disabled':'long_term_sick_or_disabled',
            'Unemployed people aged 16 to 74: Aged 16 to 24':'young_unemployed',
            'unemployment_pct':'unemployment_pct'
    }

    employment_2011 = rename_column_names(
        employment_2011,
        rename_dict=COLUMN_RENAME_DICT
    )
    employment_2011 = subset_columns(employment_2011, list(COLUMN_RENAME_DICT.values()))
    
    for col in ['caring_for_family', 'long_term_sick_or_disabled', 'young_unemployed']:
        employment_2011[col] = employment_2011[col].div(employment_2011['total_pop'])

    employment_2011 = employment_2011.drop('total_pop', axis=1)
    employment_2011['ward_name_2007'] = employment_2011['ward_name_2007'].apply(normalise_text)

    employment_2011['ward_name_2007'] = employment_2011['ward_name_2007'].map(
        lambda x:MANNUAL_WARD_NAME_EDITS_2007.get(x,x)
    )
    employment_2011 = employment_2011.merge(ward_code_2007_lookup, on='ward_name_2007', how='left').drop('ward_name_2007', axis=1)

    employment_2011['date_start'] = pd.to_datetime('2011-01-01')

    print(employment_2011.sort_values('young_unemployed', ascending=False))




if __name__ == '__main__':
    main()

