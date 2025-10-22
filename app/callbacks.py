from dash import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
from pathlib import Path
import requests
from datetime import date


from src.data_pipelines.preprocessing.spacial_processing import load_and_prepare_shapefile
from src.data_pipelines.preprocessing.utils import normalise_text
from src.DB.DatabaseClient import DatabaseReader



def register_callbacks(app):
    '''
    
    '''
    @app.callback(
        Output('crime-map', 'figure'),
        [Input('crime-map-year-selector', 'value'),
         Input('crime-map-month-selector', 'value')]
    )
    def init_crime_graph(year:int, month:int):
        database_client = DatabaseReader()

        formatted_date = date(year, month, 1).strftime("%Y-%m-%d")
        crime_data = requests.get(f'http://127.0.0.1:8000/history/crime?date={formatted_date}')
        crime_data = pd.DataFrame(crime_data.json())

        ward_name_data = pd.DataFrame(database_client.get_ward_names())
        ward_name_data = requests.get('http://127.0.0.1:8000/history/wards')
        ward_name_data = pd.DataFrame(ward_name_data.json())
        
        crime_data = crime_data.merge(ward_name_data, on='ward_code')
        crime_data['count'] = np.log(crime_data['count'])

        CURRENT_DIR = Path(__file__).resolve()
        for parent in CURRENT_DIR.parents:
            if (parent / "data").exists():
                PACKAGE_DIR = parent
                break
        else:
            raise FileNotFoundError("Could not find project root containing 'data' folder")

        shapefile_path = str(PACKAGE_DIR / 'data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp')
        shapefile = database_client.get_shapefile()
 
        # shapefile, _ = load_and_prepare_shapefile(shapefile_path, 'WD25CD', 'WD25NM', '2022', 4326, normalise_text, data=shapefile)
        geojson = shapefile.set_index("ward_code").__geo_interface__

        fig = px.choropleth_map(
            crime_data,
            geojson=geojson,
            locations="ward_code",
            color="count",
            color_continuous_scale="RdYlGn_r",
            map_style=None,  # disable basemap tiles
            zoom=6,
            center={"lat": 57.1, "lon": -4.25},
            hover_name="ward_name",
            custom_data=["ward_code"]
        )

        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",  # transparent background
            plot_bgcolor="rgba(0,0,0,0)",   # transparent plot area
            mapbox_style=None,              # make sure no tiles are drawn
            coloraxis_showscale=False
        )

        return fig
    
    @app.callback(
            Output('crime-plot', 'figure'),
            Input('crime-map', 'clickData')
    )
    def init_crime_plot(clickData):
        ward_code = 'S13002517'
        ward_name = 'Kintyre and the Islands'
        if clickData is not None:
            ward_code = clickData['points'][0]['customdata'][0] 
            ward_name = clickData['points'][0]['hovertext']
            print(f"Clicked ward: {ward_name} ({ward_code})")

        crime_data_object = requests.get(f'http://127.0.0.1:8000/history/crime?ward_code={ward_code}')
        if crime_data_object.status_code == 200:
            crime_data = crime_data_object.json()

        crime_data = pd.DataFrame(crime_data)
        
        fig = px.line(
            x=crime_data['date'],
            y=crime_data['count']
        )
        fig.update_layout(
            title=f"Crime data for ward: {ward_name}",
            xaxis_title="Area / Ward",
            yaxis_title="Incidents",
            # paper_bgcolor='#303030'
        )
        return fig

    