from fastapi import Request, APIRouter
from pydantic import BaseModel
from datetime import date
from typing import Optional


router = APIRouter(prefix="/history", tags=["history"])

class CrimeRecord(BaseModel):
    count:int
    ward_code: str
    date: date

class WardRecord(BaseModel):
    ward_code:str
    ward_name:str

class EducationRecord(BaseModel):
    ward_code:str
    pop_with_qual:float
    pop_without_qual:float
    date: date

class EmploymentRecord(BaseModel):
    ward_code:str
    date:date
    unemployed_adults:float
    long_term_sick_or_disabled:float
    caring_for_family:float

class PopulationDensityRecord(BaseModel):
    ward_code:str
    population_density:float
    date:date

@router.get('/wards', response_model=list[WardRecord])
async def get_ward_names_data(request:Request):
    db = request.app.state.crime_service.DB
    return db.get_ward_names()

@router.get('/crime', response_model=list[CrimeRecord])
async def get_ward_crime_data(request:Request, ward_code:Optional[str]=None, date:Optional[str]=None):
    db = request.app.state.crime_service.DB
    return db.get_crime_data(ward_code, date)

@router.get('/education', response_model=list[EducationRecord])
async def get_ward_education_data(request:Request, ward_code:Optional[str]=None, date:Optional[str]=None):
    db = request.app.state.crime_service.DB
    return db.get_education_data(ward_code, date)

@router.get('/employment', response_model=list[EmploymentRecord])
async def get_ward_employment_data(request:Request, ward_code:Optional[str]=None, date:Optional[str]=None):
    db = request.app.state.crime_service.DB
    return db.get_employment_data(ward_code, date)

@router.get('/population_density', response_model=list[PopulationDensityRecord])
async def get_ward_population_density_data(request:Request, ward_code:Optional[str]=None, date:Optional[str]=None):
    db = request.app.state.crime_service.DB
    return db.get_population_data(ward_code, date)