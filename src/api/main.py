from fastapi import FastAPI
from contextlib import asynccontextmanager
from pathlib import Path

from src.models.predict_model import CrimePredictor, CrimeService
from src.DB.DatabaseClient import DatabaseReader
from src.api import history
from src.api import predict


@asynccontextmanager
async def lifespan(app: FastAPI):
    '''
    
    '''
    #The the package root
    CURRENT_DIR = Path(__file__).resolve()
    for parent in CURRENT_DIR.parents:
        if (parent / "src").exists():
            PACKAGE_DIR = parent
            break
    else:
        raise FileNotFoundError("Could not find project root containing 'src' folder")
    #Find the path for the model as a string
    CrimePredictor_path = str(PACKAGE_DIR / 'src/models/linear_model.pkl')

    #Load database client and crime predictor client
    database_client = DatabaseReader()
    crime_predictor = CrimePredictor(CrimePredictor_path)

    #Initialise the crime service class and store it in the app
    crime_service = CrimeService(crime_predictor, database_client)
    app.state.crime_service = crime_service
    yield

app = FastAPI(lifespan=lifespan)

app.include_router(history.router)
app.include_router(predict.router)
