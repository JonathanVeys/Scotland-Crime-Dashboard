from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
import asyncio
from numpy import random
from src.models.predict_model import CrimePredictor


app = FastAPI()

@app.get("/")
async def root():
    return {'message':'Hello, world'}


@app.get('/predict')
async def predict(ward_code):
    predictor: CrimePredictor = app.state.pre
    crime = .predict_crime(ward_code=ward_code)
    return {ward_code:crime}


@asynccontextmanager
async def startup(app):
    async with asyncio.timeout(10):
        crime_predictor = CrimePredictor(model_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/src/models/linear_model.pkl')
        await crime_predictor.load_model()
        app.state.predictor = crime_predictor
        print("✅ Predictor loaded")
    try:
        yield 
    finally:
        async with asyncio.timeout(10):
            print("👋 Cleaned up")