import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px

import pandas as pd
from datetime import datetime 
import timestring
import numpy as np
import json

import logging

logging.getLogger().setLevel(logging.INFO)

with open('./gz_2010_us_040_00_500k.json') as json_file:
    us_states = json.load(json_file)

candidates = ['Donald Trump', 'Joe Biden', 'Biden / Trump']

data_biden = pd.read_csv('archive/hashtag_joebiden.csv', lineterminator='\n')
data_biden['created_at'] = data_biden['created_at'].apply(lambda i: timestring.Date(i).date.date())
data_biden['created_at'] = data_biden['created_at'].apply(lambda i: i.strftime('%Y-%m-%d'))

data_trump = pd.read_csv('archive/hashtag_donaldtrump.csv', lineterminator='\n')
data_trump['created_at'] = data_trump['created_at'].apply(lambda i: timestring.Date(i).date.date())
data_trump['created_at'] = data_trump['created_at'].apply(lambda i: i.strftime('%Y-%m-%d'))

logging.info('Finish reading data')

dates = np.union1d(data_trump['created_at'].unique(), data_biden['created_at'].unique())

biden_per_state_date = data_biden[data_biden.country == 'United States of America'][['created_at', 'state', 'user_followers_count']]
trump_per_state_date = data_trump[data_trump.country == 'United States of America'][['created_at', 'state', 'user_followers_count']]

states = np.union1d(trump_per_state_date['state'].astype(str).unique(), biden_per_state_date['state'].astype(str).unique())
states = states[states != 'nan']

logging.info('Finish casiting states')

def prepare_data():
    global trump_per_state_date, biden_per_state_date, dates, states
    logging.info('Finish date casting')
    biden_data_per_state = {state : biden_per_state_date[biden_per_state_date['state'] == state] for state in states}

    logging.info('Biden states info processing...')

    for state in states:

        biden_data_per_state[state] = biden_data_per_state[state].drop(['state'], axis=1).groupby('created_at').apply(lambda i: pd.Series({'views': i['user_followers_count'].sum(), 'tweets': i.size}))
        dates_to_add = np.setdiff1d(dates, biden_data_per_state[state].index)
        dates_to_add_df = pd.DataFrame({'created_at' : dates_to_add, 
                                    'views': np.zeros(len(dates_to_add)), 
                                    'tweets':np.zeros(len(dates_to_add))}).set_index('created_at')
        biden_data_per_state[state] = pd.concat([biden_data_per_state[state], dates_to_add_df]).sort_index()
        biden_data_per_state[state][['views_cumsum', 'tweets_cumsum']] = biden_data_per_state[state][['views', 'tweets']].cumsum()
        biden_data_per_state[state]['state'] = state

    data_biden_grouped = pd.concat(biden_data_per_state.values())
    data_biden_grouped.reset_index(inplace=True)

    logging.info('Finish Biden data processing')

    logging.info('Trump states info processing...')

    trump_data_per_state = {state : trump_per_state_date[trump_per_state_date['state'] == state] for state in states}

    for state in states:
        trump_data_per_state[state] = trump_data_per_state[state].drop(['state'], axis=1).groupby('created_at').apply(lambda i: pd.Series({'views': i['user_followers_count'].sum(), 'tweets': i.size}))
        dates_to_add = np.setdiff1d(dates, trump_data_per_state[state].index)
        dates_to_add_df = pd.DataFrame({'created_at' : dates_to_add, 
                                    'views': np.zeros(len(dates_to_add)), 
                                    'tweets':np.zeros(len(dates_to_add))}).set_index('created_at')
        trump_data_per_state[state] = pd.concat([trump_data_per_state[state], dates_to_add_df]).sort_index()
        trump_data_per_state[state][['views_cumsum', 'tweets_cumsum']] = trump_data_per_state[state][['views', 'tweets']].cumsum()
        trump_data_per_state[state]['state'] = state

    data_trump_grouped = pd.concat(trump_data_per_state.values())
    data_trump_grouped.reset_index(inplace=True)

    logging.info('Finish Trump data processing')

    all_data = data_trump_grouped.merge(data_biden_grouped, 
                                        on=['state', 'created_at'], 
                                        suffixes=['_Donald Trump', '_Joe Biden']
                                        )
    for i in ['tweets', 'views', 'views_cumsum', 'tweets_cumsum']:
        all_data[i + '_ratio'] = (all_data[i + '_Joe Biden'] / (all_data[i + '_Joe Biden'] + all_data['tweets_Donald Trump']))

    
    return all_data

preprocessed_data = None



for i, state in enumerate(us_states['features']):
    us_states['features'][i]['id'] = us_states['features'][i]['properties']['NAME']

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.P("Candidate:"),
    dcc.RadioItems(
        id='candidate', 
        options=[{'value': x, 'label': x} 
                 for x in candidates],
        value=candidates[0],
        labelStyle={'display': 'inline-block'}
    ),
    dcc.RadioItems(
        id='metric', 
        options=[{'value': x, 'label': x} 
                 for x in ['tweets', 'views', 'views_cumsum', 'tweets_cumsum']],
        value='tweets',
        labelStyle={'display': 'inline-block'}
    ),
    dcc.Graph(id='graph-with-slider'),
    dcc.Slider(
        id='date',
        min=0,
        max=len(dates) - 1,
        value=0,
        marks={str(i): date[5:] for i, date in enumerate(dates)},
        step=None
    )
])


@app.callback(
    [
    Output('graph-with-slider', 'figure'),
    ],
    [
    Input("candidate", "value"),
    Input("metric", "value"),
    Input("date", "value"),
    ])
def update_figure(candidate, metric, date):
    global dates
    date_str = dates[date]
    global preprocessed_data
    if preprocessed_data is None:
        preprocessed_data = prepare_data()
        logging.info('Data is ready')
    if candidate == candidates[-1]:
        candidate = 'ratio'
    metric = metric + '_' + candidate
    logging.info('Prepearing the figure...')
    fig = px.choropleth_mapbox(preprocessed_data[preprocessed_data['created_at'] == date_str], geojson=us_states, locations='state', color=metric,
                           #color_continuous_scale="Viridis",
                           #animation_frame="created_at",
                           range_color=(0, preprocessed_data[metric].max()),
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5,
                          )
    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
    )
    logging.info('Figure is ready')
    return [fig]


if __name__ == '__main__':
    app.run_server(debug=True)