from dash import dcc, html
import calendar

layout = html.Div([
    html.Div([
        html.Div([
            html.Div([
                html.H3('Ward Specific Breakdown')
            ], className='title-container'),
            html.Div([
                html.Label(
                    'Prediction month:',
                    style={
                        'font-weight': 'bold',
                        'color': 'white',
                        'font-family': 'Verdana, Geneva, Tahoma, sans-serif',
                        'whiteSpace': 'nowrap'   # ✅ stops text wrapping
                    }
                ),
                dcc.Slider(
                    id='prediction-slider',
                    min=1,
                    max=12,
                    step=1,
                    value=1,
                    marks={i:str(i) for i in range(1,13)}
                )
            ], className='slider-container', style={'width': '90%', 'margin': '0 auto'}),
            html.Div([
                dcc.Graph(
                    id='crime-plot',
                    config={"displayModeBar": False, "responsive": True},
                    style={"width": "100%", "height": "100%", "border": "0", "padding": "0", "margin": "0"}     
                )
            ], className='prediction-container', style={'width': '90%', 'margin': '0 auto'})
        ], className='crime-graph-container', style={'flex':'7'}),
        html.Div([

        ], className='crime-graph-container', style={'flex':'4'})
    ],className='container', style={'flex':'3'}),

    html.Div([
        html.Div([
            html.Div([
                html.H3('Crime Map Scotland')
            ], className='title-container'),
            html.Div([
                html.Div([
                    dcc.Dropdown(
                        id='crime-map-year-selector',
                        options = [{'label':str(year), 'value':year} for year in range(2020, 2025)],
                        value = 2020,
                        clearable=False,
                    )
                ]),
                html.Div([
                    dcc.Slider(
                        id='crime-map-month-selector',
                        min=1,
                        max=12,
                        step=1,
                        marks={i:calendar.month_name[i] for i in range(1,12)},
                        value=1
                    )
                ], className='slider-container', style={'width': '80%', 'margin': '0 auto'})
            ], style={'display':'flex', 'flex-direction':'row'}),
            dcc.Graph(
                id='crime-map',
                config={"displayModeBar": False, "responsive": True},
                style={"width": "100%", "height": "100%", "border": "0", "padding": "0", "margin": "0"}            
                )
        ], className='scotland-map-container', style={'margin-left':'10px'})
    ], className='container', style={'flex':'2'})
], className='main-container')
