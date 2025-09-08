from dash import dcc, html
import plotly.express as px

df = px.data.gapminder().query("year == 2007")

# Create a choropleth (world map)
fig = px.choropleth(
    df,
    locations="iso_alpha",    # country codes
    color="lifeExp",          # color by life expectancy
    hover_name="country",     # hover info
    color_continuous_scale=px.colors.sequential.Plasma,
    projection='natural earth'
)

fig.update_layout(coloraxis_showscale=False)
fig.update_layout(
    paper_bgcolor="#303030",
    geo=dict(
        showframe=False,        # remove frame around map
        showcoastlines=False,   # remove coastlines
        bgcolor='#303030',      # background inside map area
        landcolor="#202020",    # land color
        lakecolor="#303030",    # lakes color
        oceancolor="#303030"    # oceans color
    )
)
fig.update_layout(
    margin=dict(l=0, r=0, t=0, b=0)  # left, right, top, bottom
)

layout = html.Div([
    html.Div([
        html.Div([
            html.Div([
                html.H3('Crime Breakdown')
            ], className='title-container'),

            html.Div([

            ], className='content-container')
        ], className='crime-graph-container', style={'margin-right':'5px'}),

        html.Div([
            html.Div([
                html.H3('Map')
            ], className='title-container'),
            html.Div([
                dcc.Graph(
                    figure=fig,
                    config={"displayModeBar": False, "responsive": True},
                    style={"width": "100%", "height": "100%", "min-height": "0px"}
)
            ], className='content-container')
        ], className='scotland-map-container', style={'margin-left':'5px'})
    ], className='container', style={'flex':'2'}),

    html.Div([
        html.Div([

        ], className='pie-chart-container', style={'margin-left':'10px', 'margin-right':'5px'}),

        html.Div([  

        ], className='pie-chart-container', style={'margin-left':'5px', 'margin-right':'5px'}),

        html.Div([

        ], className='pie-chart-container', style={'margin-left':'5px', 'margin-right':'10px'})
    ], className='container', style={'flex':'1'}),

    html.Div([
        html.Div([

        ], className='data-container')
    ], className='container', style={'flex':'1'})
], className='main-container')
