from dotenv import load_dotenv 
import os

from sqlalchemy import create_engine
import pandas as pd


load_dotenv()
DB_URL = os.getenv("SUPABASE_DB_URL")

if DB_URL is not None:
    engine = create_engine(DB_URL)

query = (
    '''
    SELECT *
    FROM ward_crime wc
    JOIN ward_education_data we
    ON wc.ward_code = we.ward_code AND wc.month = we.month AND wc.year = we.year
    JOIN ward_employemnt_data wed
    ON wc.ward_code = wed.ward_code AND wc.month = wed.month AND wc.year = wed.year
    JOIN ward_population_density wd
    ON wc.ward_code = wd.ward_code AND we.month = wd.month AND wc.year = wd.year
    JOIN ward_code_name wcn
    ON wc.ward_code = wcn.ward_code;
    '''
)
df = pd.read_sql(query, engine)
df_columns = list(set(df.columns))
df = df.loc[:, ~df.columns.duplicated()]
print(df[df['ward_code'] == 'S13002516'].sort_values(['year', 'month']))
