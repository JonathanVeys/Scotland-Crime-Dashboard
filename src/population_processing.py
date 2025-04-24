import pandas as pd
import matplotlib.pyplot as plt

def population_data_ingestion(file_path:str) -> pd.DataFrame:
    '''
    Read and processing the population data for the Scottish Councils. Uses the area data of the Scottish councils to turn the population data into population density data.

    Parameters:
    -file_path: File path to file containing the population data.

    Returns:
    -data: A pandas dataframe containing the population density data for each council in Scotland (Population per km^2)
    '''

    #Read the population data into python
    try:
        population_df = pd.read_excel(file_path, sheet_name='Table 1', skiprows=5)
    except Exception as e:
        raise ValueError(f'Error reading population excel file: {e}')
    
    #Filter out rows that only count male and female populations
    population_df = population_df[population_df['Sex'] == 'Persons']

    #Keep the rows that contain the data we need
    population_df = population_df[['Area name', 'Year', 'All Ages']].sort_values(by=['Area name', 'Year']).reset_index(drop=True)

    #Read the Council Area data into python
    try:
        area_df = pd.read_csv('/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Council_Area_Data.csv', dtype={
            'Area name':str,
            'Area':float
        })
    except Exception as e:
        raise ValueError(f'Error in reading area data excel file: {e}')

    #Merge the population data with the area data
    population_df = pd.merge(population_df, area_df, on='Area name')

    #Set the column order
    population_df = population_df[['Area name', 'Area', 'Year', 'All Ages']]

    #Calculate the population density
    population_df['All Ages'] = population_df['All Ages'] / population_df['Area']

    return population_df


if __name__ == '__main__':
    population_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Population Data/Scotland_Population_Data_1981-2023.xlsx'
    processed_population_data = population_data_ingestion(population_path)
