from bs4 import BeautifulSoup
import requests
import pandas as pd
from io import BytesIO


def get_ward_excel_link(soup:BeautifulSoup, search_id:str, searching_types:list|str = ['excel', 'xlsx']) -> str | None:
    '''
    Finds and returns the first Excel file link from a BeautifulSoup-parsed page,
    based on a given class name used for identifying links.
    '''
    results = soup.find_all('a', class_=search_id)

    for result in results:
        href = result.get('href')
        if href and any(ext in href.lower() for ext in searching_types):
            return href
    
    return None

def read_excel_sheets(content:bytes, skiprows_num=0, sheet_name = None):
    '''
    Reads an Excel file from bytes and returns all sheets as a dictionary of DataFrames.
    '''
    return pd.read_excel(BytesIO(content))

def download_excel(href:str, sheet_name:str|None = None, skip_rows:int = 0):
    '''
    Downloads the content of an Excel file from a given URL.
    '''
    if href is not None:
        r = requests.get(href)
        data = read_excel_sheets(r.content, sheet_name=sheet_name, skiprows_num=skip_rows)
        return data
    else:
        return None
    
