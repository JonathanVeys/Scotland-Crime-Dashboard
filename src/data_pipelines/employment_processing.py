import pandas as pd


def data_ingestion(data_path:str, skip_rows:int) -> pd.DataFrame:
    '''

    '''

    if not data_path.endswith('.csv'):
        raise ValueError('Error: File must be of type csv')
    
    try:
        data = pd.read_csv(data_path, skiprows=skip_rows)
    except Exception as e:
        raise ValueError(f'Error: {e}')
    
    if pd.isna(data.iloc[0,0]):
        data = data.iloc[:,1:]

    if pd.isna(data.iloc[0,-1]):
        data = data.iloc[:,:-1]

    data = data.dropna()

    data.columns.values[0] = 'Ward_name'

    data = data.sort_values(by='Ward_name').reset_index(drop=True)

    return data

def column_idx_finder(column_names:list, search_word:str) -> int:
    return next(i for i, item in enumerate(column_names) if search_word in item)

def subset_column(data:pd.DataFrame, column_names_seach:list) -> list:
    column_names = list(data.columns)
    return [column_idx_finder(column_names, column_name) for column_name in column_names_seach]


def employment_processing(data:pd.DataFrame, column_search_strings:list) -> pd.DataFrame:
    column_idx = subset_column(data, column_search_strings)
    data = data.iloc[:,column_idx]
    return data


def percentage_employment(data:pd.DataFrame) -> pd.DataFrame:
    total_population_idx = column_idx_finder(list(data.columns), 'aged')
    full_time_employed_dx = column_idx_finder(list(data.columns), 'Unemployed')

    data['Percentage_employed'] = data.iloc[:,full_time_employed_dx] / data.iloc[:, total_population_idx]

    return data

def normalise_column (data:pd.DataFrame, column_index:int) -> pd.DataFrame:
    data.iloc[:, column_index] = (data.iloc[:, column_index] - data.iloc[:, column_index].min()) / (data.iloc[:, column_index].max() - data.iloc[:, column_index].min())

    return data


path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Employment Data/Census_Employment_2011.csv'
data_2011 = data_ingestion(path, 11)
data_processed_2011 = employment_processing(data_2011, list(('Ward_name', 'aged', 'Unemployed')))

path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Employment Data/Census_Employment_2022.csv'
data_2022 = data_ingestion(path, 10)
data_processed_2022 = employment_processing(data_2022, list(('Ward_name', 'aged', 'Unemployed')))

data_percentage_2022 = percentage_employment(data_processed_2022).sort_values(by='Ward_name', ascending=True).reset_index(drop=True)
data_percentage_2011 = percentage_employment(data_processed_2011).sort_values(by='Ward_name', ascending=True).reset_index(drop=True)

print(data_percentage_2011.head())
print('=====')
print(data_percentage_2022.head())


wards_2022 = list(data_percentage_2022.iloc[:, 0])
wards_2011 = list(data_percentage_2011.iloc[:, 0])


non_matching = [ward for ward in wards_2022 if ward.strip().lower() not in [w.strip().lower() for w in wards_2011]]
print(sorted(non_matching))
print(len(wards_2011))
print(len(non_matching))


print('Abbey' in wards_2011)
print('Abbey' in wards_2022)