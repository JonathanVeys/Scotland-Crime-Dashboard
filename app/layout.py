from dash import dcc, html

layout = html.Div([
    html.Div([
        dcc.Slider(
            id='monthly_slider',
            marks={
                1:'Jan', 2:'Feb', 3:'Mar',
                4:'Apr', 5:'May', 6:'Jun',
                7:'Jul', 8:'Aug', 9:'Sep',
                10:'Oct', 11:'Nov', 12:'Dec'},
                step=1
        )
    ], className='input-box'),

    html.Div([
        html.Div([
            dcc.Graph(
                id='crime_map',
                figure = {}, 
                className='scotland-map',
                config = {'displayModeBar': False}
                )
        ], className='choropleth-map-div'),
        html.Div([
            html.Div([
                dcc.Graph(
                    id='crime_breakdown_pie',
                    figure={},
                    # className='pie-chart',
                    config={'displayModeBar': False}
                    )
            ], className='crime-breakdown-div'),
            html.Div([
                dcc.Graph(
                    id='council_crime_timeseries',
                    figure={},
                    # className='timeseries-graph',
                    config={'displayModeBar': False}
                    )
            ], className='timeseries-div')
        ], className='graphs-div')
    ], className='output-box')
], className='main-container')


