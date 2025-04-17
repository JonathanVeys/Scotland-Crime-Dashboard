import dash
from dash import dcc, html


app = dash.Dash(__name__)

app.layout = html.Div([
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
        ], className='choropleth-map-div'),
        html.Div([
            html.Div([

            ], className='crime-breakdown-div'),
            html.Div([

            ], className='timeseries-div')
        ], className='graphs-div')
    ], className='output-box')
], className='main-container')


if __name__ == "__main__":
    app.run(debug=True)