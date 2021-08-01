import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
import numpy as np
from dash.dependencies import Output, Input
import dash_table
import firebaselinker as fl
xp=1

fl.changevalues()

filteredata=fl.query1('icfCm43hMbXSmZy3WhZt')
filteredata2=fl.query2('icfCm43hMbXSmZy3WhZt')
filteredata3=fl.query3('icfCm43hMbXSmZy3WhZt')
filteredata4=fl.query4()
filteredata5=fl.query5()
filteredata6=fl.query6('icfCm43hMbXSmZy3WhZt')
filteredata10=fl.query10()
filteredata11=fl.query11('icfCm43hMbXSmZy3WhZt')

px.set_mapbox_access_token("pk.eyJ1IjoiYXNhZDUzIiwiYSI6ImNrcTI3b3d2ZzAweXIydnVzcXRlcWE1eDQifQ.bYrnwp7Bdc4Qnz5OYAHtDQ")

#print(type(data))
external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css2?"
        "family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server
app.title = "Doodel Analytics"

app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.Div([
                html.Button('Refresh', id='submit-val', n_clicks=0,style={'color':'white'}),
                html.Div(id='container-button-basic',style={'color':'white'},
                    children='')
                ]),
                html.P(children="🥑", className="header-emoji"),
                html.H1(
                    children="Doodel Analytics", className="header-title"
                ),
                html.P(
                    children="Analyze App performance live through Firebase Database",
                    className="header-description",
                ),
            ],
            className="header",
        ),
        html.Div(
            children=[

                html.Div(
                    children=[
                        html.Div(children="Store Id", className="menu-title"),
                        dcc.Dropdown(
                            id="type-filter",
                            options=[
                                {"label": avocado_type, "value": avocado_type}
                                for avocado_type in fl.df["ShopName"].unique()
                            ],
                            style={'width':'300px'},
                            value="Flogsta Närlivs",
                            clearable=False,
                            searchable=False,
                            className="dropdown",
                        ),
                    ]
                ),
            ],
            className="menu",
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="price-chart",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
                html.H3("Prices Sold At"),
                html.Div(
                    title='Prices Sold At',
                    children=dash_table.DataTable(
                        id='tablepricesold',
                        columns=[{"name": i, "id": i} for i in filteredata2.columns],
                        data=filteredata2.to_dict('records'),
                        page_action='none',
                        style_table={'height': '300px', 'overflowY': 'auto'},
                        style_cell={'textAlign': 'left',
                                    'backgroundColor': 'rgb(50, 50, 50)',
                                    'color': 'white'
                                    },
                        style_cell_conditional=[
                            {
                                'if': {'column_id': 'DateTime'},
                                'textAlign': 'left'
                            }
                        ],
                        style_header={'backgroundColor': 'rgb(30, 30, 30)', 'border': '1px solid pink'},
                        style_data={'border': '1px solid blue'}
                    ),
                    className="card",
                ),
                html.H3("Time Sold At"),
                html.Div(
                    title='Time Sold At',
                    children=dash_table.DataTable(
                    id='tabletimesold',
                    columns=[{"name": i, "id": i} for i in filteredata3.columns],
                    data=filteredata3.to_dict('records'),
                    page_action='none',
                    style_table={'height': '300px', 'overflowY': 'auto'},
                    style_cell={'textAlign': 'left',
                                'backgroundColor': 'rgb(50, 50, 50)',
                                'color': 'white'
                                },
                    style_cell_conditional=[
                    {
                    'if': {'column_id': 'DateTime'},
                    'textAlign': 'left'
                    }
                    ],
                    style_header={'backgroundColor': 'rgb(30, 30, 30)','border': '1px solid pink'},
                    style_data={ 'border': '1px solid blue' }
                    ),
                    className="card",
                ),
                html.H3("Customer Address"),
                html.Div(
                    children=dcc.Graph(
                    id='tableaddress',
                    figure=px.scatter_mapbox(filteredata4, lat="Latitude", lon="Longitude",color="OrderFrequency",hover_name="CustomerName",size="AverageSubtotal",
                    color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=10)
                    ),
                    className="card",
                ),
                html.H3("Store Orders"),
                html.Div(
                    children=dash_table.DataTable(
                        id='tablestorerating',
                        columns=[{"name": i, "id": i} for i in filteredata5.columns],
                        data=filteredata5.to_dict('records'),
                        page_action='none',
                        style_table={'height': '200px', 'overflowY': 'auto'},
                        style_cell={'textAlign': 'left',
                                    'backgroundColor': 'rgb(30, 30, 30)',
                                    'color': 'white'
                                    },
                        style_cell_conditional=[
                            {
                                'if': {'column_id': 'Address'},
                                'textAlign': 'left'
                            }
                        ],
                        style_header={'backgroundColor': 'rgb(30, 30, 30)', 'border': '1px solid pink'},
                        style_data={'border': '1px solid blue'}
                    ),
                    className="card",
                ),
                html.H3("Day Performance"),
                html.Div(
                    children=dash_table.DataTable(
                        id='tabletimeroutine',
                        columns=[{"name": i, "id": i} for i in filteredata6.columns],
                        data=filteredata6.to_dict('records'),
                        page_action='none',
                        style_table={'height': '170px', 'overflowY': 'auto'},
                        style_cell={'textAlign': 'left',
                                    'backgroundColor': 'rgb(30, 30, 30)',
                                    'color': 'white'
                                    },
                        style_cell_conditional=[
                            {
                                'if': {'column_id': 'Address'},
                                'textAlign': 'left'
                            }
                        ],
                        style_header={'backgroundColor': 'rgb(30, 30, 30)', 'border': '1px solid pink'},
                        style_data={'border': '1px solid blue'}
                    ),
                    className="card",
                ),
                html.H3("User Orders"),
                html.Div(
                    children=dash_table.DataTable(
                        id='tableuserating',
                        columns=[{"name": i, "id": i} for i in filteredata10.columns],
                        data=filteredata10.to_dict('records'),
                        page_action='none',
                        style_table={'height': '300px', 'overflowY': 'auto'},
                        style_cell={'textAlign': 'left',
                                    'backgroundColor': 'rgb(30, 30, 30)',
                                    'color': 'white'
                                    },
                        style_cell_conditional=[
                            {
                                'if': {'column_id': 'Address'},
                                'textAlign': 'left'
                            }
                        ],
                        style_header={'backgroundColor': 'rgb(30, 30, 30)', 'border': '1px solid pink'},
                        style_data={'border': '1px solid blue'}
                    ),
                    className="card",
                ),
                html.H3("Categories Orders"),
                html.Div(
                    title='Categories Orders',
                    children=dash_table.DataTable(
                    id='tablecat',
                    columns=[{"name": i, "id": i} for i in filteredata11.columns],
                    data=filteredata11.to_dict('records'),
                    page_action='none',
                    style_table={'height': '120px', 'overflowY': 'auto'},
                    style_cell={'textAlign': 'left',
                                'backgroundColor': 'rgb(50, 50, 50)',
                                'color': 'white'
                                },
                    style_cell_conditional=[
                    {
                    'if': {'column_id': 'DateTime'},
                    'textAlign': 'left'
                    }
                    ],
                    style_header={'backgroundColor': 'rgb(30, 30, 30)','border': '1px solid pink'},
                    style_data={ 'border': '1px solid blue' }
                    ),
                    className="card",
                ),

            ],
            className="wrapper",
        ),
    ]
)


@app.callback(
    [Output("price-chart", "figure")],
    [
        Input("type-filter", "value")
    ],
)

def update_charts(avocado_type):
    res = fl.df
    resid = res['ShopId']
    resname = res['ShopName']
    for r in range(len(resname)):
        if resname[r] == avocado_type:
            avocado_type = resid[r]
            break
        else:
            pass
    filtered_data = fl.query1(avocado_type)
    filtered_data['ProductName']

    price_chart_figure = {
        "data": [
            {
                "x": filtered_data['ProductName'],
                "y": filtered_data['counts'],
                "type": "lines",
                "hovertemplate": "%{y:.2f}<extra></extra>",
            },
        ],
        "layout": {
            "title": {
                "text": "Products Sold",
                "x": 0.05,
                "xanchor": "left",
            },
            "xaxis": {"fixedrange": True},
            "yaxis": {"tickprefix": "", "fixedrange": True},
            "colorway": ["#17B897"],
        },
    },


    return price_chart_figure

@app.callback(
    [Output("tabletimesold", "data")],
    [
        Input("type-filter", "value")
    ],
)

def update_tabletimesold(avocado_type):
    res=fl.df
    resid=res['ShopId']
    resname=res['ShopName']
    for r in range(len(resname)):
        if resname[r]==avocado_type:
            avocado_type=resid[r]
            break
        else:
            pass
    filteredatatimesold = fl.query3(avocado_type)
    return [filteredatatimesold.to_dict('records')]


@app.callback(
    [Output("tablecat", "data")],
    [
        Input("type-filter", "value")
    ],
)

def update_tablecategories(avocado_type):
    res = fl.df
    resid = res['ShopId']
    resname = res['ShopName']
    for r in range(len(resname)):
        if resname[r] == avocado_type:
            avocado_type = resid[r]
            break
        else:
            pass
    filteredatacat = fl.query11(avocado_type)
    return [filteredatacat.to_dict('records')]

@app.callback(
    [Output("tablepricesold", "data")],
    [
        Input("type-filter", "value")
    ],
)

def update_tablecategories(avocado_type):
    res = fl.df
    resid = res['ShopId']
    resname = res['ShopName']
    for r in range(len(resname)):
        if resname[r] == avocado_type:
            avocado_type = resid[r]
            break
        else:
            pass
    filteredataprice = fl.query2(avocado_type)
    return [filteredataprice.to_dict('records')]


@app.callback(
    [Output("tabletimeroutine", "data")],
    [
        Input("type-filter", "value")
    ],
)

def update_tablecategories(avocado_type):
    res = fl.df
    resid = res['ShopId']
    resname = res['ShopName']
    for r in range(len(resname)):
        if resname[r] == avocado_type:
            avocado_type = resid[r]
            break
        else:
            pass
    filteredataprice = fl.query6(avocado_type)
    return [filteredataprice.to_dict('records')]


@app.callback(
    dash.dependencies.Output('container-button-basic', 'children'),
    [dash.dependencies.Input('submit-val', 'n_clicks')])
def update_output(n_clicks):
    fl.changevalues()
    return 'The Page Has Been Refreshed {} times'.format(
        n_clicks
    )

if __name__ == "__main__":
    app.run_server(debug=True)