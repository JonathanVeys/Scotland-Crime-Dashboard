from dash import Input, Output
import plotly.express as px
import pandas as pd
import numpy as np
import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
from pathlib import Path


from src.data_pipelines.preprocessing.spacial_processing import load_and_prepare_shapefile
from src.data_pipelines.preprocessing.utils import normalise_text
from src.DB.DatabaseClient import DatabaseClient



def register_callbacks(app):
    '''
    
    '''
    @app.callback(
        Output('crime-map', 'figure'),
        Input('crime-map', 'id')
    )
    def init_map(selected_date):
        database_client = DatabaseClient()
        crime_data = pd.DataFrame(database_client.get_crime_data())
        ward_name_data = pd.DataFrame(database_client.get_ward_names())
        crime_data = crime_data.merge(ward_name_data, on='ward_code')
        crime_data['count'] = np.log(crime_data['count'])

        CURRENT_DIR = Path(__file__).resolve()
        PACKAGE_DIR = CURRENT_DIR.parent.parent

        shapefile_path = str(PACKAGE_DIR / 'data/geojson_data/scottish_wards_2022_shapefile/Wards_(May_2025)_Boundaries_UK_BFC_(V2).shp')
        shapefile, _ = load_and_prepare_shapefile(shapefile_path, 'WD25CD', 'WD25NM', '2022', 4326, normalise_text)
        geojson = shapefile.set_index("ward_code_2022").__geo_interface__

        # fig = px.choropleth_map(
        #     crime_data,
        #     geojson=geojson,
        #     locations="ward_code",
        #     color="count",
        #     color_continuous_scale="RdYlGn_r",
        #     map_style="carto-positron",
        #     zoom=5.5, center={"lat": 57.1, "lon": -4.25},
        #     hover_name="ward_name",  # what appears on hover
        #     custom_data=["ward_code"]  # pass extra info to hoverData
        # )

        # fig.update_layout(
        #     margin=dict(l=5, r=5, t=5, b=5),  
        #     mapbox=dict(style="white-bg"),
        #     coloraxis_showscale=False,
        #     map=dict(style="white-bg"),   # no basemap tiles
        #     paper_bgcolor="rgba(0,0,0,0)",   # transparent overall background
        #     plot_bgcolor="rgba(0,0,0,0)"
        # )

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

    