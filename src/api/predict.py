from fastapi import Request, APIRouter
from pydantic import BaseModel


router = APIRouter(prefix="/predict", tags=["predict"])

class CrimePrediction(BaseModel):
    crime_prediction: float

@router.get('/crime/{ward_code}', response_model=CrimePrediction)
async def predict_crime(request:Request, ward_code:str, months:int=1):
    predictor = request.app.state.crime_service
    return predictor.predict(ward_code, months=months)