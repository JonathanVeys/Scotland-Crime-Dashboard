import pandas as pd
from sqlalchemy import create_engine


def validate_columns(data:pd.DataFrame, required_columns:list) -> bool:
    required_columns = required_columns
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        return False
    else:
        return True
    
def update_db(data:pd.DataFrame, db_url:str, table_name:str, required_columns:list):
    try:
        engine = create_engine(db_url)
    except Exception as ex:
        raise ValueError(f"Error: Failed to generate engine due to: {ex}")


    if not validate_columns(data, required_columns):
        raise ValueError("Error: Data has missing columns")
    
    try: 
        data.to_sql(table_name, engine, if_exists='replace', index=False)
        print("✅Successfully updated database values")

    except Exception as ex:
        print(f"Error: Update failed due to: {ex}")




