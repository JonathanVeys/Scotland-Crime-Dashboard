from dash import Dash
from layout import layout

app = Dash(__name__)
app.layout = layout

if __name__ == "__main__":
    app.run(debug=True)