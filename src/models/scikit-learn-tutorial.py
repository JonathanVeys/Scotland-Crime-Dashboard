import sklearn as skl
from dotenv import load_dotenv 
import os

import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pandas as pd


load_dotenv()
DB_URL = os.getenv("SUPABASE_DB_URL")

if DB_URL is not None:
    engine = create_engine(DB_URL)

query = (
    '''
    SELECT *
    FROM ward_education_data;
    '''
)
df = pd.read_sql(query, engine)

for ward_code, group_data in df.groupby(['ward_code']):
    plt.plot(group_data['date'], group_data['pop_with_qual'])
    plt.plot(group_data['date'], group_data['pop_without_qual'])
    plt.show()
    break