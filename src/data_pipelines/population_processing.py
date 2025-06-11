import pandas as pd


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

    #Sord the data and reset the index
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



if __name__ == '__main__':
    population_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Population Data/Scottish_Ward_Population.xlsx'
    processed_population_data = population_data_ingestion(population_path)
    
    ward_area_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Population Data/Electoral_Wards_Size.csv'
    processed_ward_area_data = ward_area_ingestion(ward_area_path)

    processed_population_density_data = population_density_processing(processed_population_data, processed_ward_area_data)
    print(processed_population_density_data)
