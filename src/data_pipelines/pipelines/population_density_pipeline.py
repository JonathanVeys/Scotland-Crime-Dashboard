import re
from typing import Dict, List
from io import BytesIO
import os

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv

from src.data_pipelines.preprocessing.base_pipeline import BasePipeline
from src.data_pipelines.preprocessing.utils import expand_date_range, interpolate_df, interpolate_forward
from src.data_pipelines.DB.update_database import update_db


class PopulationDensityPipeline(BasePipeline):
    def __init__(self, data:pd.DataFrame):
        super().__init__(data)

    @classmethod
    def from_path(cls, path:str):
        data = pd.read_csv(path)

        return cls(data)

    @classmethod
    def from_scrape(cls, base_url:str, page_url:str, year:str):
        '''
        A function that allows the class to initiliased from scraping instead of by dataframe
        '''

        #Generate the soup object for the url page
        url = urljoin(base=base_url, url=page_url)
        response = cls._url_response(url)
        soup = cls._generate_soup(response.content)

        #Retrieve bytes for xlsx file
        page_data_url = cls._extract_data_url(soup, '.xlsx$')
        data_url = urljoin(base=base_url, url=page_data_url)
        data_bytes = cls._url_response(data_url)

        #Load the data into a dataframe
        data = pd.read_excel(BytesIO(data_bytes.content), sheet_name=None)

        if year in data.keys():
            return cls(data[year])
        else:
            raise KeyError(f'Year {year} not present in data keys, only {list(data.keys())}')


    @staticmethod
    def _url_response(URL):
        '''
        Gets a response from a URL
        '''
        response = requests.get(URL)
        if response.status_code == 200:
            return response
        else:
            print(response.status_code)
            raise ValueError(f'Status Code: {response.status_code}')
    
    @staticmethod
    def _generate_soup(content:bytes, parser:str='html.parser'):
        '''
        Creates a bs4 soup object
        '''
        if isinstance(content, str):
            soup = BeautifulSoup(content, parser)
        else:
            raise TypeError(f'Expected type bytes, instead got type ({type(content)})')
        return soup
    
    @staticmethod
    def _extract_data_url(soup:BeautifulSoup, regex_expression:str, match_index:int=0):
        '''
        Searches a beautiful soup object from an a tag with some regex expression in is href
        '''
        if not isinstance(soup, BeautifulSoup):
            raise TypeError(f'Expected type BeautifulSoup, instead got ({type(soup)})')
        
        result = soup.find_all(name='a', attrs={'href':re.compile(regex_expression)})
        
        if match_index >= len(result):
            raise ValueError(f'match_index {match_index} is out of bounds - only {len(result)} matches found')
        if result:
            return result[match_index]['href']
        else:
            raise ValueError('No matches found')
        
    
    def find_new_colnames_index(self, regex_exp:str):
        '''
        
        '''
        col_names_index = np.nan
        for index, row in self.data.iterrows():
            for _, value in row.items():
                value = str(value)
                if re.search(regex_exp, value):
                    col_names_index = index
                    return col_names_index
                
    def find_new_colnames(self, regex_exp:str):
        '''
        
        '''
        col_names_index = self.find_new_colnames_index(regex_exp)
        if isinstance(col_names_index, int):
            new_col_names = self.data.iloc[col_names_index,:].values   
            self.rename_cols({col_old:col_new for col_old, col_new in zip(self.data.columns, new_col_names)})

            #Dropping columns above the new column names
            self.data = self.data.iloc[(col_names_index+1):, :]
        return self
    
    def filter_df(self, filter_col:str, filter_values:List[str]|str):
        self._check_list_subset(filter_col, list(self.data.columns))

        if isinstance(filter_values, str):
            filter_values = [filter_values]

        for filter_value in filter_values:    
            self.data = self.data[self.data[filter_col].str.contains(filter_value, case=False)].reset_index(drop=True)
        return self
    
    def set_date_column(self, date_col_name:str, date:str):
        self.data[date_col_name] = pd.to_datetime(date)
        return self


def main():
    base_url = "https://www.nrscotland.gov.uk"
    page_url = f"{base_url}/publications/electoral-ward-population-estimates/"

    population_data_2000 = PopulationDensityPipeline.from_scrape(base_url, page_url, '2001')
    population_data_2000 = (
        population_data_2000.find_new_colnames('Electoral Ward 2022 Name')
        .drop_columns([col for col in population_data_2000.data.columns if 'Age' in col])
        .filter_df('Sex', 'Persons')
        .drop_columns(['Sex', 'Electoral Ward 2022 Name'])
        .rename_cols({'Electoral Ward 2022 Code':'ward_code', 'Total':'total_population'})
        .set_date_column('date', '2001-01-01')
        .extract_df()
    )

    population_data_2025 = PopulationDensityPipeline.from_scrape(base_url, page_url, '2021')
    population_data_2025 = (
        population_data_2025.find_new_colnames('Electoral Ward 2022 Name')
        .drop_columns([col for col in population_data_2025.data.columns if 'Age' in col])
        .filter_df('Sex', 'Persons')
        .drop_columns(['Sex', 'Electoral Ward 2022 Name'])
        .rename_cols({'Electoral Ward 2022 Code':'ward_code', 'Total':'total_population'})
        .set_date_column('date', '2021-01-01')
        .extract_df()
    )

    ward_area_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/raw_yearly_population_data/Electoral_Wards_Size.csv'
    ward_area_data = PopulationDensityPipeline.from_path(ward_area_path)
    ward_area_data = (
        ward_area_data.filter_df('WD24CD', 'S')
        .drop_columns([col for col in ward_area_data.data.columns if col not in ['WD24CD', 'Shape__Area', 'date']])
        .rename_cols({'WD24CD':'ward_code', 'Shape__Area':'area'})
        .normalise_column(normalise_func=lambda x: x/1000000, col_to_normalise='area')
        .extract_df()
    )

    population_data_2000 = population_data_2000.merge(ward_area_data, on='ward_code', how='left')
    population_data_2025 = population_data_2025.merge(ward_area_data, on='ward_code', how='left')
    population_density_data = pd.concat([population_data_2000, population_data_2025], ignore_index=True)
    population_density_data['population_density'] = population_density_data['total_population'] / population_density_data['area']
    population_density_data = population_density_data.drop(['total_population', 'area'], axis=1)

    population_density_data_list = []
    for _, group_df in population_density_data.groupby(['ward_code']):
        df = expand_date_range(group_df, 'date', 'ward_code')
        df = interpolate_df(df, ['population_density'])
        df = interpolate_forward(df, 'date', ['population_density'], pd.to_datetime('2025-01-01'))
        population_density_data_list.append(df)

    population_density_data = pd.concat(population_density_data_list, ignore_index=True)
    
    load_dotenv()
    DB_URL = os.getenv("SUPABASE_DB_URL")

    required_columns = [
        'ward_code',
        'population_density',
        'date'
    ]

    if DB_URL is not None:
        update_db(data=population_density_data, db_url=DB_URL, table_name='ward_population_density', required_columns=required_columns)



    
if __name__ == '__main__':
    main()