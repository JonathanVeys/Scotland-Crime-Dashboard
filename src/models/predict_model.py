import joblib
from numpy import log1p
from dotenv import load_dotenv 
import os
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import asyncio

from sklearn.linear_model import LinearRegression

# model_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/src/models/linear_model.pkl'
# query = """
# SELECT *
# FROM ward_crime wc
# JOIN ward_education_data we
# ON wc.ward_code = we.ward_code AND wc.date = we.date
# JOIN ward_employemnt_data wed
# ON wc.ward_code = wed.ward_code AND wc.date = wed.date
# JOIN ward_population_density wd
# ON wc.ward_code = wd.ward_code AND wc.date = wd.date;
# """

# load_dotenv()
# DB_URL = os.getenv("SUPABASE_DB_URL")
# if DB_URL is None:
#     raise ValueError("SUPABASE_DB_URL not found in environment variables.")

# engine = create_engine(DB_URL)

# df = pd.read_sql(query, engine)

# Remove duplicate columns
# df = df.loc[:, ~df.columns.duplicated()]

# # Ensure date is datetime
# df['date'] = pd.to_datetime(df['date'])


# def predict_crime(
#         ward_code:str,
#         predict_window:int=1
# ):
#     model = joblib.load(model_path)
#     historic_data = df[df['ward_code'] == ward_code]
#     historic_data.set_index("date", inplace=True)
#     next_month = historic_data.index[-1] + pd.DateOffset(months=1)

#     interpolated_data = {}
#     for col in historic_data.select_dtypes(include=[np.number]).columns:
#         # x = month index (0,1,2,...)
#         x = np.arange(len(historic_data)).reshape(-1,1)
#         y = historic_data[col].values
#         model_lr = LinearRegression().fit(x, y)
        
#         next_val = model_lr.predict([[len(historic_data)]])[0]
#         interpolated_data[col] = next_val

#     historic_data.loc[next_month] = interpolated_data
#     historic_data.loc[next_month, 'ward_code'] = ward_code


#     # Lag features
#     historic_data['crime_last_month'] = historic_data.groupby('ward_code')['count'].shift(1)
#     historic_data['crime_last_two_months'] = historic_data.groupby('ward_code')['count'].shift(2)
#     historic_data['crime_last_three_months'] = historic_data.groupby('ward_code')['count'].shift(3)

#     # # Rolling average (optional)
#     historic_data['crime_3month_avg'] = (
#         historic_data.groupby('ward_code')['count']
#         .transform(lambda x: x.shift(1).rolling(3).mean())
#     )
#     # # Drop rows with missing lag features
#     historic_data = historic_data.dropna(subset=['crime_last_three_months']).reset_index(drop=True)
#     X = historic_data.drop(['count', 'ward_code'], axis=1)
#     X['pop_density_log'] = np.log1p(X['population_density'])
#     X.drop('population_density', axis=1, inplace=True)
#     X = X.iloc[[-1]]

#     prediction = model.predict(X)
#     return prediction[0]

# predict_crime('S13002517')

    


class ward_predictor():
    def __init__(self, weights_path:str, ward_code:str) -> None:
        with open(weights_path, 'rb') as f:
            self.model = joblib.load(f)

        self.historic_df = self.load_ward_data(ward_code)
        pass

    def load_ward_data(self, ward_code:str):
        query = text(
            """
            SELECT 
                wc.ward_code,
                wc.date,
                wc.count,
                we.pop_with_qual,
                we.pop_without_qual,
                wed.unemployed_adults,
                wed.long_term_sick_or_disabled,
                wed.caring_for_family,
                wd.population_density
            FROM ward_crime wc
            JOIN ward_education_data we
            ON wc.ward_code = we.ward_code AND wc.date = we.date
            JOIN ward_employemnt_data wed
            ON wc.ward_code = wed.ward_code AND wc.date = wed.date
            JOIN ward_population_density wd
            ON wc.ward_code = wd.ward_code AND wc.date = wd.date
            WHERE wc.ward_code = :ward_code;
            """
            )
       
        load_dotenv()
        DB_URL = os.getenv("SUPABASE_DB_URL")
        if DB_URL is None:
            raise ValueError("SUPABASE_DB_URL not found in environment variables.")

        engine = create_engine(DB_URL)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"ward_code": ward_code})

        print(df)
        return df
    

class CrimePredictor():
    def __init__(self, model_path:str) -> None:
        self.path = model_path
        self.model = None

    async def load_model(self):
        # run joblib.load in a thread to avoid blocking event loop
        self.model = await asyncio.to_thread(joblib.load, self.path)
    
    def load_historic_data(self, ward_code:str):
        query = text(
            """
            SELECT 
                wc.ward_code,
                wc.date,
                wc.count,
                we.pop_with_qual,
                we.pop_without_qual,
                wed.unemployed_adults,
                wed.long_term_sick_or_disabled,
                wed.caring_for_family,
                wd.population_density
            FROM ward_crime wc
            JOIN ward_education_data we
            ON wc.ward_code = we.ward_code AND wc.date = we.date
            JOIN ward_employemnt_data wed
            ON wc.ward_code = wed.ward_code AND wc.date = wed.date
            JOIN ward_population_density wd
            ON wc.ward_code = wd.ward_code AND wc.date = wd.date
            WHERE wc.ward_code = :ward_code;
            """
            )
       
        load_dotenv()
        DB_URL = os.getenv("SUPABASE_DB_URL")
        if DB_URL is None:
            raise ValueError("SUPABASE_DB_URL not found in environment variables.")

        engine = create_engine(DB_URL)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"ward_code": ward_code})

        return df

    def generate_features(self, ward_code:str, months:int):
        historic_df = self.load_historic_data(ward_code)
        
        pass

    
    # def predict_crime(self, ward_code:str, months:int=1):
    #     X = self.generate_features(ward_code, months)
    #     return self.model.predict(X)

    
model_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/src/models/linear_model.pkl'
ward_code = 'S13002517'

wp = ward_predictor(model_path, ward_code)
