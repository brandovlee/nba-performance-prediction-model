# Baseline model for predicting player points in NBA games (using only PPG as the predictor)
# MAE: 4.75
import mysql.connector
import os
from contextlib import contextmanager
from dotenv import load_dotenv
import pandas as pd
import random
from datetime import date
from datetime import timedelta
import sys
import pickle
from functools import reduce

# Import additional libraries for modeling
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
import numpy as np
import matplotlib.pyplot as plt

@contextmanager
def connect_to_sql():
    """Connects to the SQL using contextmanager to efficiently manage the connection and cursor"""
    conn = None
    try:
        # Load the .env file
        load_dotenv()

        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = conn.cursor()
        yield cursor, conn  # Yield both cursor and connection to use inside the `with` block
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:  # Close cursor and conn after usage
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_data_from_sql(query):
    """Loads data from the MySQL database"""
    with connect_to_sql() as (cursor, conn):
        cursor.execute(query)
        columns = [i[0] for i in cursor.description]
        data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)

def get_opp_team_and_home_advantage(team, nba_matchups_df):
    if team in nba_matchups_df['Home_Team'].values:
        opp_team = nba_matchups_df.loc[nba_matchups_df['Home_Team'] == team, 'Away_Team'].values[0]
        home_advantage = 1
    elif team in nba_matchups_df['Away_Team'].values:
        opp_team = nba_matchups_df.loc[nba_matchups_df['Away_Team'] == team, 'Home_Team'].values[0]
        home_advantage = 0
    else:
        opp_team = None
        home_advantage = 0
    return opp_team, home_advantage

def append_days_since_last_game(df):
    # Import boxscore data
    query = "SELECT * FROM player_boxscore"
    boxscores_df = load_data_from_sql(query)
    boxscores_df['Player'] = boxscores_df['Player'].apply(lambda x: x.replace(' Jr.', ''))

    # Get the last game date for each player
    boxscores_df['Date'] = pd.to_datetime(boxscores_df['Date'])

    # Get the last game date for each player
    Last_Game_Date = boxscores_df.groupby(['Team', 'Player'])['Date'].max().reset_index().rename(columns={'Date': 'Last_Game_Date'})

    # Merge the last game date with the original dataframe
    df = df.merge(Last_Game_Date, on=['Team', 'Player'], how='left')
    df['Date1'] = pd.to_datetime(df['Date'])
    df['Days_Since_Last_Game'] = (df['Date1'] - df['Last_Game_Date']).dt.days
    
    # Append is_back_to_back
    df['Is_Back_To_Back'] = df['Days_Since_Last_Game'].apply(lambda x: 1 if x == 1 else 0)

    # Clean up
    df.drop(columns=['Date1', 'Last_Game_Date'], inplace=True)

    return df

def process_injury_data(df):
    # Load injury report
    query = "SELECT * FROM injury_report"
    injured_players = load_data_from_sql(query)
    injured_players.drop(columns=['id'], inplace=True)

    # Merge injured players with player data
    injured_df = df.merge(injured_players, on=['Team', 'Player', 'Date'], how='inner')

    # Group by 'Date' and 'Team' and calculate the sum of all numeric columns
    aggregated_df = (
        injured_df
        .groupby(['Date', 'Team'])
        .sum(numeric_only=True)
        .reset_index()
    )

    # Add suffix to distinguish new aggregated columns
    aggregated_df = aggregated_df.add_suffix('_unknown')
    aggregated_df.rename(columns={'Date_unknown': 'Date', 'Team_unknown': 'Team'}, inplace=True)

    # Merge the aggregated data back to the original dataframe
    df = df.merge(aggregated_df, on=['Date', 'Team'], how='left')

    return df

def preprocess_data(player_data, player_tables, opp_data, opp_tables):
    # Merge all player_tables
    player_df = reduce(lambda left, right: pd.merge(left, right, on=['Team', 'Player', 'Date'], how='outer'), player_data.values())
    player_df.fillna(0, inplace=True)
    player_df['Player'] = player_df['Player'].apply(lambda x: x.replace(' Jr.', ''))
    player_df = process_injury_data(player_df) # Process injury data

    # Merge all opp_tables
    opp_df = reduce(lambda left, right: pd.merge(left, right, on=['Opp_Team', 'Date'], how='inner'), opp_data.values())

    # Merge nba_matchups with player_df to get test_df
    today_str = (date.today()).strftime('%Y-%m-%d')  
    test_df = player_df[player_df['Date'].astype(str) == today_str].copy()
    query = "SELECT * FROM nba_matchups"
    nba_matchups_df = load_data_from_sql(query)
    test_df[['Opp_Team', 'Home_Court_Advantage']] = test_df['Team'].apply(
        lambda x: pd.Series(get_opp_team_and_home_advantage(x, nba_matchups_df))
    )
    test_df = test_df.merge(opp_df, on=['Opp_Team', 'Date'], how='inner')
    test_df = test_df.dropna(subset=['Opp_Team'])

    # Append days since last game
    test_df = append_days_since_last_game(test_df)

    return test_df

def predict_on_real_data(test_df, features, model):
    # Merge dk props with today's data
    query = "SELECT * FROM dk_props"
    dk_props = load_data_from_sql(query)
    dk_props['Player'] = dk_props['Player'].apply(lambda x: x.replace(' Jr.', ''))
    test_df = test_df.merge(dk_props.drop(columns='id'), on=['Player'], how='inner')
    
    # Prepare input features
    input_features = test_df[features]

    # Predict
    predicted_points = model.predict(input_features)
    test_df['Predicted_Points'] = predicted_points.round(2)
    test_df['Difference'] = (test_df['Predicted_Points'] - test_df['Line']).round(2)
    test_df['Difference_PPG'] = (test_df['Predicted_Points'] - test_df['PPG']).round(2)
    test_df['Difference_Line'] = (test_df['Predicted_Points'] - test_df['Line']).round(2)

    # Save the results (sorted by difference)
    test_df = test_df.sort_values(by='Difference_PPG', ascending=False)
    projections = test_df[['Player', 'Opp_Team', 'Predicted_Points',  'Line', 'PPG', 'Difference_Line', 'Difference_PPG']]
    projections.to_csv('projections.csv', index=False)

if __name__ == '__main__':
    # Define the tables to be used
    player_tables = ['player_playtype', 'player_misc', 'player_shot_locations', 'player_traditional']
    opp_tables = ['opp_playtype', 'opp_misc', 'opp_shot_locations', 'opp_traditional']

    # Load player data from SQL
    player_data = {}
    for table in player_tables:
        query = f"SELECT * FROM {table}"
        player_data[table] = load_data_from_sql(query)
        player_data[table]['Date'] = (pd.to_datetime(player_data[table]['Date']) + timedelta(days=1)).dt.date # Shift the date by 1 day
        if 'id' in player_data[table].columns:
            player_data[table] = player_data[table].drop(columns=['id'])
        if 'GameID' in player_data[table].columns:
            player_data[table] = player_data[table].drop(columns=['GameID'])

    # Load opponent data from SQL
    opp_data = {}
    for table in opp_tables:
        query = f"SELECT * FROM {table}"
        opp_data[table] = load_data_from_sql(query)
        opp_data[table] = opp_data[table].rename(columns={'Team': 'Opp_Team'})
        opp_data[table]['Date'] = (pd.to_datetime(opp_data[table]['Date']) + timedelta(days=1)).dt.date # Shift the date by 1 day
        if 'id' in opp_data[table].columns:
            opp_data[table] = opp_data[table].drop(columns=['id'])

    # Split the training and testing sets
    test_df = preprocess_data(player_data, player_tables, opp_data, opp_tables)
    test_df.to_csv('test_df.csv', index=False)

    # Create feature sets
    playtype = ['2FGA_cns', '3PA_cns', '2FGA_pullup', '3PA_pullup']

    zone = ['RA_FGA', 'Mid_FGA', 'LC3_FGA', 'RC3_FGA', 'C3_FGA', 'AB3_FGA']

    opp_playtype = ['Opp_2FGA_cns', 'Opp_3PA_cns', 'Opp_2FGA_pullup', 'Opp_3PA_pullup']

    opp_zone = ['Opp_RA_FGA', 'Opp_Mid_FGA', 'Opp_LC3_FGA', 'Opp_RC3_FGA', 'Opp_C3_FGA', 'Opp_AB3_FGA']
    
    misc = ['PTS_OFF_TOV', 'PTS_2ND_CHANCE', 'PTS_FB', 'PTS_PAINT']

    opp_misc = ['OPP_PTS_OFF_TOV', 'OPP_PTS_2ND_CHANCE', 'OPP_PTS_FB', 'OPP_PTS_PAINT']

    injury = ['2FGA_cns_unknown', '3PA_cns_unknown', '2FGA_pullup_unknown', '3PA_pullup_unknown',
            'RA_FGA_unknown', 'Mid_FGA_unknown', 'LC3_FGA_unknown',  'RC3_FGA_unknown', 
            'C3_FGA_unknown', 'AB3_FGA_unknown', 'PTS_OFF_TOV_unknown',
            'PTS_2ND_CHANCE_unknown', 'PTS_FB_unknown', 'PTS_PAINT_unknown']

    traditional = ['AGE', 'W_PCT', 'MIN', 'FGA', '3PA', 'FTA', 'FG_PCT', '3P_PCT']

    opp_traditional = ['OPP_FGM', 'OPP_FGA', 'OPP_3PA', 'OPP_FTA', 'OPP_FG_PCT', 'OPP_3P_PCT']

    context = ['Is_Back_To_Back', 'Home_Court_Advantage']

    # Define features and label
    features = playtype + zone + misc + traditional + opp_playtype + opp_zone + opp_misc + opp_traditional + injury  + context

    # Import the model
    model = pickle.load(open('model.pkl', 'rb'))

    # Predict
    predict_on_real_data(test_df, features, model)
    print("Projections saved to projections.csv")


