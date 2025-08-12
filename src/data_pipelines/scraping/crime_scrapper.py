import requests
from typing import List, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from . import utils


def get_crime_data_url(base_url:str, page_url:str) -> list:
    '''
    A function that scrapes a URL for crime data URLs
    '''

    response = requests.get(urljoin(base_url, page_url))

    soup = BeautifulSoup(response.content, 'html.parser')
    crime_urls = [(url.get('title'), base_url + url.get('href')) for url in soup.find_all('a') if url.get('title') and 'Detected' in url.get('title')]

    return crime_urls


def crime_data_scrapper(crime_urls:List[Tuple[str, str]]) -> dict:
    crime_data = dict()

    for title, url in crime_urls:
        df = utils.download_excel(url, sheet_name='Sheet1')
        crime_data[title] = df

    return crime_data


