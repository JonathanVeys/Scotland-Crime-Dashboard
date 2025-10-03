import os
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


class DatabaseClient:
    def __init__(self, DB_URL:str|None=None) -> None:
        load_dotenv()
        self.db_url = DB_URL or os.getenv('SUPABASE_DB_URL')
        if not self.db_url:
            raise ValueError('UPABASE_DB_URL not found in environment variables.')
        
        self.engine = create_engine(
            url = self.db_url,   
        )

    def fetch(self, sql:str, params:dict|None=None) -> List[dict]:
        '''
        
        '''

        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params).to_dict(orient='records')
        
    def get_crime_data(self, ward_code:str|None=None, date:str|None=None) -> List[dict]:
        '''
        
        '''
        sql = '''
            SELECT * 
            FROM ward_crime
            WHERE (:ward_code IS NULL or ward_code = :ward_code) 
            AND (:date IS NULL or date = :date)
        '''
        return self.fetch(sql, {'ward_code':ward_code, 'date':date})
    
    def get_ward_names(self) -> List[dict]:
        '''
        
        '''
        sql = '''
            SELECT ward_code, ward_name
            FROM ward_code_name
        '''
        return self.fetch(sql)
    
    def get_education_data(self, ward_code:str|None=None, date:str|None=None) -> List[dict]:
        '''
        
        '''

        sql = '''
            SELECT *
            FROM ward_education_data
            WHERE (:ward_code IS NULL or ward_code = :ward_code)
            AND (:date IS NULL or date = :date)
        '''

        return self.fetch(sql, {'ward_code':ward_code, 'date':date})
    
    def get_employment_data(self, ward_code:str|None=None, date:str|None=None) -> List[dict]:
        '''
        
        '''
        sql = '''
            SELECT *
            FROM ward_employemnt_data
            WHERE (:ward_code IS NULL or ward_code = :ward_code)
            AND (:date IS NULL or date = :date)
        '''
        
        return self.fetch(sql, {'ward_code':ward_code, 'date':date})
    

    def get_population_data(self, ward_code:str|None=None, date:str|None=None) -> List[dict]:
        '''
        
        '''
        sql = '''
            SELECT * 
            FROM ward_population_density
            WHERE (:ward_code IS NULL or ward_code = :ward_code)
            AND (:date IS NULL or date = :date)
        '''
        
        return self.fetch(sql, {'ward_code':ward_code, 'date':date})