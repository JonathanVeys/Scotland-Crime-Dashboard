import requests
import os
import pandas as pd

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv

from scraping.utils import get_ward_excel_link, download_excel, read_excel_sheets
from preprocessing.data_preprocessing import ward_pop_processing, ward_area_processing
from DB.update_database import update_db

def ward_pop_gather() -> dict:
    '''
    Function that webscrapes nrscotland to gather ward population data.

    Parameters:
    - There are no parameters for this function.

    Output:
    - Returns a dictionary of ward data population where each entry in the dictionary is a year.
    '''

    base_url = "https://www.nrscotland.gov.uk"
    page_url = f"{base_url}/publications/electoral-ward-population-estimates/"

    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, "html.parser")

    file_url = get_ward_excel_link(soup, 'ds_file-download__title')
    file_url = urljoin(base_url, file_url)

    if not file_url:
        raise ValueError('Error: File URL is None')

    data = download_excel(file_url, sheet_name='2001')
    print(data)
    
    # if content is not None:
    #     data = read_excel_sheets(content, 3)
    # else:
    #     raise ValueError('Error: Content cannot be empty')

    return data

def ward_size_gather() -> pd.DataFrame:
    '''
    Function that webscrappes data.gov to get the area data for the electoral wards.

    Parameters:
    - There are no parameters for this function

    Output:
    - Returns a pandas dataframe that links each ward in the UK to a specifc Shape__Area
    '''

    base_url = "https://www.data.gov.uk"
    page_url = f"{base_url}/dataset/0bdfd7a6-e6a4-4d63-a684-b6dda1d86d47/wards-may-2024-boundaries-uk-bsc"

    response = requests.get(page_url)
    
    soup = BeautifulSoup(response.content, "html.parser")

    file_url = get_ward_excel_link(soup, 'govuk-link')

    if not file_url:
        raise ValueError("Error: File URL is None")
    
    content = download_excel(file_url)
    print(content)

    if content is not None:
        print(content)
        data = read_excel_sheets(content, 0)
    else:
        raise ValueError("Error: Content cannot be empty")

    key = list(data.keys())[0]
    return data[key]

def main():
    ward_population_data = ward_pop_gather()
    ward_population_data = ward_pop_processing(ward_population_data)  

    ward_area_data = pd.read_csv('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/raw_yearly_population_data/Electoral_Wards_Size.csv')
    ward_area_data = ward_area_processing(ward_area_data)

    ward_density_data = pd.merge(ward_population_data, ward_area_data, on='Electoral Ward 2022 Code')

    ward_density_data.rename(columns={'Electoral Ward 2022 Code':'ward_code', 'Date':'date', 'Ward_Name':'ward_name'}, inplace=True)

    ward_density_data['population_density'] = ward_density_data['Total'] / ward_density_data['Shape__Area']

    ward_density_data = ward_density_data.drop(['Total', 'Shape__Area'], axis=1)

    ward_density_data = ward_density_data[['ward_code', 'ward_name', 'date', 'population_density']]

    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'ward_name',
        'date',
        'population_density'
    ]

    if DB_URL is not None:
        update_db(data=ward_density_data, db_url=DB_URL, table_name='ward_population_density', required_columns=required_columns)



if __name__ == '__main__':
    main()