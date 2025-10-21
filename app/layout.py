from dash import dcc, html

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
            dcc.Graph(
                id='crime-map',
                config={"displayModeBar": False, "responsive": True},
                style={"width": "100%", "height": "100%", "border": "0", "padding": "0", "margin": "0"}            
                )
        ], className='scotland-map-container', style={'margin-left':'10px'})
    ], className='container', style={'flex':'2'})
], className='main-container')
