import joblib
import pandas as pd
import numpy as np
import asyncio
from typing import List

from sklearn.linear_model import LinearRegression
from dateutil.relativedelta import relativedelta

from src.DB.DatabaseClient import DatabaseClient



class CrimePredictor():
    def __init__(self, model_path:str) -> None:
        self.path = model_path
        self.model = self.load_model()

    def load_model(self):
        model = joblib.load(self.path)
        return model

    def generate_prediction_row(self, data:pd.DataFrame) -> pd.DataFrame:
        '''
        
        '''
        numeric_cols = data.select_dtypes(include='number').columns
        new_row ={
            col:None if col in numeric_cols else data.loc[len(data)-1, col] for col in data.columns
        }
        new_row['date'] = new_row["date"] + relativedelta(months=1) #type:ignore


        for idx, col in enumerate(numeric_cols):
            if col == 'count':
                continue
            X = np.arange(len(data.loc[:,col])).reshape(-1,1)
            y = data.loc[:,col]
            model = LinearRegression().fit(X,y)

            next_val = model.predict(np.array([[len(data)]]))[0]
            new_row[col] = next_val

        data.loc[len(data)] = new_row #type:ignore
        return data

    def data_preprocessing(self, data:pd.DataFrame) -> pd.DataFrame:
        '''
        
        '''
        #Calculate lag values
        data['crime_last_month'] = data['count'].shift(1)
        data['crime_last_two_months'] = data['count'].shift(2)
        data['crime_last_three_months'] = data['count'].shift(3)
        #Apply a log function to population density
        data['pop_density_log'] = np.log1p(data['population_density'])
        data.drop(labels='population_density', axis=1, inplace=True)
        #Calculate the 3 month rolling average of crime
        data["crime_3month_avg"] = data["count"].shift(1).rolling(window=3).mean()

        data = data[~pd.isna(data["crime_last_three_months"])]    
        prediction_row = data.iloc[[-1], 1:]

        return prediction_row
    

    def predict(self, predict_row:dict) -> dict:
        '''
        Function that predicts upcoming crime rates.

        Input:
            -prediction_row (dict): 

        Output:
            -A number representing the the predicted crime next month in based on the data
        '''

        df = pd.DataFrame([predict_row])
        df.drop(columns=['ward_code', 'date'], axis=1, inplace=True)
        df = df[['pop_with_qual', 'pop_without_qual', 'unemployed_adults', 'long_term_sick_or_disabled', 'caring_for_family', 'crime_last_month', 'crime_last_two_months', 'crime_last_three_months', 'crime_3month_avg', 'pop_density_log']]
        output = self.model.predict(df)

        return {'crime_prediction':float(output)}
    

class CrimeService():
    def __init__(self, predictor:CrimePredictor, DBClient:DatabaseClient):
        self.predictor = predictor

        self.DB = DBClient

    def get_ward_history(self, ward_code:str) -> List[dict]:
        ward_crime_data = pd.DataFrame(self.DB.get_crime_data(ward_code=ward_code))
        ward_education_data = pd.DataFrame(self.DB.get_education_data(ward_code=ward_code))
        ward_employment_data = pd.DataFrame(self.DB.get_employment_data(ward_code=ward_code))
        ward_population_data = pd.DataFrame(self.DB.get_population_data(ward_code=ward_code))


        ward_history = (
            ward_crime_data
            .merge(ward_education_data, on=['ward_code', 'date'], how="inner")
            .merge(ward_employment_data, on=['ward_code', 'date'], how="inner")
            .merge(ward_population_data, on=['ward_code', 'date'], how="inner")
        ).to_dict(orient='records')

        return ward_history
    
    def predict(self, ward_code:str, months:int) -> dict:
        historic_data = self.get_ward_history(ward_code)
        prediction_row = self.predictor.generate_prediction_row(pd.DataFrame(historic_data))
        prediction_row_processed = self.predictor.data_preprocessing(prediction_row)
        predicted_crime = self.predictor.predict(prediction_row_processed)

        return predicted_crime

        