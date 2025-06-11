from dash import Output, Input
import json
import plotly.express as px
import numpy as np
from src.data_pipelines.population_processing import population_data_ingestion, ward_area_ingestion, population_density_processing



def register_callbacks(app):
    crime_map_callback(app)


def crime_map_callback(app):
    @app.callback(
        Output('crime_map', 'figure'),
        Input('monthly_slider', 'value')
    )
    def generate_crime_graph(month):
        with open('/Users/jonathancrocker/Downloads/georef-united-kingdom-ward-electoral-division.geojson') as f:
            geojson_data = json.load(f)

        population_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Population Data/Scottish_Ward_Population.xlsx'
        processed_population_data = population_data_ingestion(population_path)

        ward_area_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Population Data/Electoral_Wards_Size.csv'
        processed_ward_area_data = ward_area_ingestion(ward_area_path)

        processing_population_density_data = population_density_processing(processed_population_data, processed_ward_area_data)
        processing_population_density_data['Population_Density_log'] = np.log(processing_population_density_data['Population_Density'])

        # Now, let's plot the choropleth map with Plotly
        fig = px.choropleth_map(
            processing_population_density_data,
            geojson=geojson_data,
            locations='Ward_Code',         # column in df
            featureidkey='properties.wed_code',   # key in GeoJSON features
            color='Population_Density_log',              # the color will be based on the 'Size' column
            color_continuous_scale=['green', 'limegreen', 'yellow', 'red', 'darkred'],     # Choose your color scale
            # map_style='carto-positron',
            zoom=6,
            center={"lat": 58, "lon": -4.2026},  # Centering the map over Scotland
            opacity=0.6,
            hover_name='Ward_Name',
            hover_data={
                'Population_Density':True,
                'Population_Density_log':False,
                'Ward_Code':False
            }
        )

        # Remove extra margin around the map
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0},
                        coloraxis_showscale=False)
        
        return fig