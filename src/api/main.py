from fastapi import FastAPI
from contextlib import asynccontextmanager
from pathlib import Path

from src.models.predict_model import CrimePredictor, CrimeService
from src.DB.DatabaseClient import DatabaseClient
from src.api import history
from src.api import predict


@asynccontextmanager
async def lifespan(app: FastAPI):
    #Load DatabaseClient
    database_client = DatabaseClient()

    #Load CrimePredictor
    PACKAGE_DIR = Path(__file__).resolve().parent.parent.parent
    CrimePredictor_path = str(PACKAGE_DIR / 'src/models/linear_model.pkl')
    crime_predictor = CrimePredictor(CrimePredictor_path)

    crime_service = CrimeService(crime_predictor, database_client)
    app.state.crime_service = crime_service
    yield

app = FastAPI(lifespan=lifespan)

app.include_router(history.router)
app.include_router(predict.router)
