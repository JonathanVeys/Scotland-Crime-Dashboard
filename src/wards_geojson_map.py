import json
import plotly.express as px
import pandas as pd

# Load the GeoJSON data
with open('/Users/jonathancrocker/Downloads/georef-united-kingdom-ward-electoral-division.geojson') as f:
    geojson_data = json.load(f)

population_data = pd.read_excel('/Users/jonathancrocker/Downloads/estimated-population-by-sex-single-year-of-age-and-electoral-ward-mid-2001-to-mid-2021.xlsx', sheet_name='2021', skiprows=3)

# Create an empty DataFrame with 'Code' and 'Size' columns
data = pd.DataFrame(columns=['Code', 'Size'])

# Iterate through the features in GeoJSON and fill the DataFrame
for feature in geojson_data['features']:
    properties = feature['properties']['wed_name'][0]  # Extract the 'wed_code'
    new_row = {'Code': properties, 'Size': 1}  # 'Size' is set to 1 for now
    data = pd.concat([data, pd.DataFrame([new_row])], ignore_index=True)

# Now, let's plot the choropleth map with Plotly
fig = px.choropleth_mapbox(
    population_data,
    geojson=geojson_data,
    locations='Electoral Ward 2022 Code',         # column in df
    featureidkey='properties.wed_code',   # key in GeoJSON features
    color='Total',              # the color will be based on the 'Size' column
    color_continuous_scale="Viridis",     # Choose your color scale
    mapbox_style="carto-positron",        # OpenStreetMap as the base map style
    zoom=5.5,
    center={"lat": 56.4907, "lon": -4.2026},  # Centering the map over Scotland
    opacity=0.6,
    hover_name='Electoral Ward 2022 Name'
)

# Remove extra margin around the map
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

# # Show the map
fig.show()
