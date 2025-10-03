from dash import dcc, html
import plotly.express as px


from dash import Input, Output
import plotly.express as px
import requests
import pandas as pd
import numpy as np
import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
from pathlib import Path


from src.data_pipelines.preprocessing.spacial_processing import load_and_prepare_shapefile
from src.data_pipelines.preprocessing.utils import normalise_text




layout = html.Div([
    html.Div([
        html.Div([
            html.Div([
                html.H3('Ward Specific Breakdown')
            ], className='title-container')
        ], className='crime-graph-container', style={'flex':'7'}),
        html.Div([

        ], className='crime-graph-container', style={'flex':'4'})
    ],className='container', style={'flex':'3'}),

    html.Div([
        html.Div([
            html.Div([
                html.H3('Crime Map Scotland')
            ], className='title-container'),
            dcc.Graph(
                id='crime-map',
                config={"displayModeBar": False, "responsive": True},
                style={"width": "100%", "height": "100%", "border": "0", "padding": "0", "margin": "0"}            
                )
        ], className='scotland-map-container', style={'margin-left':'10px'})
    ], className='container', style={'flex':'2'})
], className='main-container')
