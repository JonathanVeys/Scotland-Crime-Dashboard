from dash import Dash
from .layout import layout
from .callbacks import register_callbacks

app = Dash(__name__)
app.layout = layout

register_callbacks(app)
server = app.server

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8051, debug=True)

