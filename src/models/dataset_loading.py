from dotenv import load_dotenv 
import os

from sqlalchemy import create_engine
import pandas as pd


load_dotenv()
DB_URL = os.getenv("SUPABASE_DB_URL")

if DB_URL is not None:
    engine = create_engine(DB_URL)

query = 'SELECT * FROM ward_employemnt_data LIMIT 100'
df = pd.read_sql(query, engine)
print(df)