import os
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from dotenv import load_dotenv

class DatabasePushError(BaseException):
    def __init__(self, message:str, code:int = 500, details:dict|None=None) -> None:
        super().__init__(message)
        self.code = code
        self.details = details

class BaseDatabaseClient:
    def __init__(self, DB_URL:str|None=None) -> None:
        load_dotenv()
        self.db_url = DB_URL or os.getenv('SUPABASE_DB_URL')
        if not self.db_url:
            raise ValueError('UPABASE_DB_URL not found in environment variables.')
        
        self.engine = create_engine(
            url = self.db_url,   
        )

class DatabaseReader(BaseDatabaseClient):
    def __init__(self, DB_URL:str|None=None) -> None:
        super().__init__(DB_URL)

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
    
class DatabaseWriter(BaseDatabaseClient):
    def __init__(self, DB_URL: str | None = None) -> None:
        super().__init__(DB_URL)

    def update_database(self, data:pd.DataFrame, table_name:str):
        try:
            data.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print("✅Successfully updated database values")
        except IntegrityError as e:
            print(f'IntegrityError: Duplicate or invalid data for {table_name}')
            raise
        except OperationalError as e:
            print('"OperationalError: Could not reach database - {e}')
            raise
        except SQLAlchemyError as e:
            print('Database error: {e}')
            raise

