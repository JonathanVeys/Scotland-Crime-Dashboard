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


if __name__ == '__main__':
    raw_ward_data = ward_pop_gather()
    procesed_ward_data = ward_pop_processing(raw_ward_data)
    print(procesed_ward_data)





def population_data_ingestion(data_path:str) -> pd.DataFrame:
    '''
    Reads and pre-processes the raw population data and outputs a processed population dataset as one continuous timeseries.

    Paremeters:
    -data_path: Path pointing to raw population data.

    Output:
    -processed_population_data: Returns a timeseries of the population of each electoral ward for each year from 2001 to 2021.
    '''
    try:
        data_sheets = pd.ExcelFile(data_path)
        sheet_names = data_sheets.sheet_names[2:]
        data = {sheet: data_sheets.parse(sheet, skiprows=3) for sheet in sheet_names}
    except Exception as e:
        raise ValueError(f'Could not load data: {e}')
    
    data = pd.concat(
        data.values(), 
        keys=data.keys(), 
        names=["Year"]).reset_index(level=0)
    
    #Selected the required columns
    data = data[['Year', 'Electoral Ward 2022 Name', 'Electoral Ward 2022 Code', 'Sex', 'Total']]

    #Filter the data to include only data for all sexes
    data = data[data['Sex'] == 'Persons'].drop('Sex', axis=1)

    #Sort the data and reset the index
    data = data.sort_values(['Electoral Ward 2022 Name', 'Year']).reset_index(drop=True)

    #Renamed the columns
    processed_population_data = data.rename(columns={'Electoral Ward 2022 Name':'Ward_Name', 'Electoral Ward 2022 Code':'Ward_Code'})

    return(processed_population_data)



def ward_area_ingestion(data_path:str) -> pd.DataFrame:
    '''
    Reads and pre-processes the ward area data and returns the processed data from the scotland wards.

    Parameters:
    -data_path: Path pointing to the raw area data.

    Output:
    -data: Returns a dataframe with each ward and its respective area in km².
    '''

    try:
        data = pd.read_csv(data_path)
    except Exception as e:
        raise ValueError(f'Error: Could not load ward area data:{e}')
    
    data = data[['WD24CD', 'WD24NM', 'Shape__Area']]

    data = data[data['WD24CD'].str.startswith('S')].reset_index(drop=True)

    data['Shape__Area'] = data['Shape__Area'] / 1e+6

    data.rename(columns={'WD24CD':'Ward_Code', 'WD24NM':'Ward_Name'}, inplace=True)

    return data

def population_density_processing(population_data:pd.DataFrame, area_data:pd.DataFrame) -> pd.DataFrame:
    '''
    Combines the output from ward_area_ingestion and population_data_ingestion to calculate a population density for each electoral ward in Scotland.

    Parameters:
    -Population_data: A pandas dataframe with a timeseries for the population for each Electoral Ward.
    -area_data: A pandas dataframe with the area in km² for each Scottish Electoral Ward.

    Output:
    -data: Output pandas dataframe with the a timeseries for the population density for each Scottish Electoral Ward.
    '''

    #Merge the population and area data into one dataframe
    data = pd.merge(population_data, area_data, on='Ward_Code')

    #Calculate the population density by dividing the total population for each Electoral Ward by it's area
    data['Total'] = data['Total'] / data['Shape__Area']

    #Select the necessary columns
    data = data[['Year', 'Ward_Name_x', 'Ward_Code', 'Total']]

    #Rename the columns to make them more clear
    data.rename(columns={'Ward_Name_x':'Ward_Name', 'Total':'Population_Density'}, inplace=True)

    return data



# if __name__ == '__main__':
#     population_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Population Data/Scottish_Ward_Population.xlsx'
#     processed_population_data = population_data_ingestion(population_path)
    
#     ward_area_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Population Data/Electoral_Wards_Size.csv'
#     processed_ward_area_data = ward_area_ingestion(ward_area_path)

#     processed_population_density_data = population_density_processing(processed_population_data, processed_ward_area_data)
#     print(processed_population_density_data)