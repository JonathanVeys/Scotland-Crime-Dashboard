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

def interpolate_column(df:pd.DataFrame, column_to_interpolate:str, grouping_column:str):
    '''
    Function that interpolates a column in a dataframe to fill in missing data
    '''
    df = df.set_index('Date')

    interpolated_df = (
        df[column_to_interpolate]
        .groupby(df[grouping_column])
        .resample('M')
        .mean()
        .interpolate(method='linear')
        .reset_index()
    )

    return interpolated_df

def ward_pop_processing(ward_pop:dict) -> pd.DataFrame:
    '''
    Data pipeline that takes the raw ward population data and create a time series, creating data for each month
    '''
    processed_ward_pop = {k:df[['Electoral Ward 2022 Name', 'Electoral Ward 2022 Code', 'Sex', 'Total']].assign(Date = k) for k,df in ward_pop.items() if str(k).isdigit()}

    processed_ward_pop = pd.concat(processed_ward_pop.values(), ignore_index=True)
    
    processed_ward_pop['Date'] = pd.to_datetime(processed_ward_pop['Date'])

    processed_ward_pop = interpolate_column(processed_ward_pop, 'Total', 'Electoral Ward 2022 Code')

    return processed_ward_pop

def ward_area_processing(data:pd.DataFrame) -> pd.DataFrame:
    '''
    Reads and pre-processes the ward area data and returns the processed data from the scotland wards.

    Parameters:
    -data_path: Path pointing to the raw area data.

    Output:
    -data: Returns a dataframe with each ward and its respective area in km².
    '''
    
    data = data[['WD24CD', 'WD24NM', 'Shape__Area']]

    data = data[data['WD24CD'].str.startswith('S')].reset_index(drop=True)

    data['Shape__Area'] = data['Shape__Area'] / 1e+6

    data.rename(columns={'WD24CD':'Electoral Ward 2022 Code', 'WD24NM':'Ward_Name'}, inplace=True)

    return data


def main():
    raw_ward_data = ward_pop_gather()
    processed_ward_data = ward_pop_processing(raw_ward_data)
    
    raw_ward_area = ward_size_gather()
    processed_ward_area = ward_area_processing(raw_ward_area)

    processed_ward_area['Electoral Ward 2022 Code'] = processed_ward_area['Electoral Ward 2022 Code'].astype(str)
    processed_ward_data['Electoral Ward 2022 Code'] = processed_ward_data['Electoral Ward 2022 Code'].astype(str)

    pop_den_data = processed_ward_data.merge(processed_ward_area, on='Electoral Ward 2022 Code')
    pop_den_data['Population_Density'] = pop_den_data['Total'] / pop_den_data['Shape__Area']

    pop_den_data = pop_den_data[['Electoral Ward 2022 Code', 'Ward_Name', 'Date', 'Total', 'Shape__Area', 'Population_Density']]

    print(pop_den_data.sort_values('Population_Density'))
    print(pop_den_data[pop_den_data['Population_Density'] == pop_den_data['Population_Density'].max()])

    return([pop_den_data])

if __name__ == '__main__':
    main()

