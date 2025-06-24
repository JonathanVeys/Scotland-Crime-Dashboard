from bs4 import BeautifulSoup
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import urljoin

def get_ward_excel_link(soup:BeautifulSoup, search_id:str) -> str | None:
    '''
    Finds and returns the first Excel file link from a BeautifulSoup-parsed page,
    based on a given class name used for identifying links.
    '''
    results = soup.find_all('a', class_=search_id)

    for result in results:
        href = result.get('href')
        if href and any(ext in href.lower() for ext in ['excel', 'xlsx']):
            return href
    
    return None


def download_excel(href:str) -> bytes | None:
    '''
    Downloads the content of an Excel file from a given URL.
    '''
    if href is not None:
        r = requests.get(href)
        return r.content
    else:
        return None
    
def read_excel_sheets(content:bytes, skiprows_num) -> dict:
    '''
    Reads an Excel file from bytes and returns all sheets as a dictionary of DataFrames.
    '''
    return pd.read_excel(BytesIO(content), sheet_name=None, skiprows=skiprows_num)

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

    content = download_excel(file_url)
    
    if content is not None:
        data = read_excel_sheets(content, 3)
    else:
        raise ValueError('Error: Content cannot be empty')

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
    
    if response.status_code != 200:
        print(response.status_code)
        print(response.headers)
        raise ValueError(f'Error: Status code:{response.status_code}')

    soup = BeautifulSoup(response.content, "html.parser")

    file_url = get_ward_excel_link(soup, 'govuk-link')

    if not file_url:
        raise ValueError("Error: File URL is None")
    
    content = download_excel(file_url)

    if content is not None:
        data = read_excel_sheets(content, 0)
    else:
        raise ValueError("Error: Content cannot be empty")

    key = list(data.keys())[0]
    return data[key]




if __name__ == '__main__':
    data = ward_pop_gather()
    print(data['2020'].head(10))

    # data = ward_size_gather()
    # print(data.head(10))