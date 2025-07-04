import os
from dotenv import load_dotenv
from population_processing import main as main_pop_dens
import pandas as pd
from sqlalchemy import create_engine


load_dotenv()
DB_URL = os.getenv("SUPABASE_DB_URL")

required_columns = [
    'Electoral_Ward_2022_Code',
    'Ward_Name',
    'Date',
    'Total',
    'Shape__Area',
    'Population_Density'
]

def validate_columns(data:pd.DataFrame, required_columns:list) -> bool:
    required_columns = required_columns
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        return False
    else:
        return True
    
def update_db(data:pd.DataFrame, db_url:str):
    try:
        engine = create_engine(db_url)
    except Exception as ex:
        raise ValueError(f"Error: Failed to generate engine due to: {ex}")


    if not validate_columns(data, required_columns):
        raise ValueError("Error: Data has missing columns")
    
    try: 
        data.to_sql('electoral_ward_population_density', engine, if_exists='replace', index=False)
        print("Successfully updated database values")

    except Exception as ex:
        print(f"Error: Update failed due to: {ex}")


def main():
    data = main_pop_dens()[0]
    if DB_URL is not None:
        update_db(data, DB_URL)

if __name__ == '__main__':
    main()
