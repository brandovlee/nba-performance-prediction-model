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

def append_days_since_last_game(df):
    # Append days since last game
    df['Date1'] = pd.to_datetime(df['Date'])
    df = df.sort_values(by=['Player', 'Date1'])
    df['Previous_Game_Date'] = (
        df
        .groupby('Player')['Date1']
        .shift(1)
    )
    df['Days_Since_Last_Game'] = (df['Date1'] - df['Previous_Game_Date']).dt.days
    df.dropna(subset=['Days_Since_Last_Game'], inplace=True)

    # Append is_back_to_back
    df['Is_Back_To_Back'] = df['Days_Since_Last_Game'].apply(lambda x: 1 if x == 1 else 0)

    # Clean up
    df.drop(columns=['Date1', 'Previous_Game_Date'], inplace=True)

    return df

def process_injury_data(df):
    # Load injury data
    query = "SELECT * FROM player_injuries"
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

    # Merge boxscore with player_df to get train_df
    query = "SELECT * FROM player_boxscore"
    boxscore = load_data_from_sql(query)
    boxscore['Player'] = boxscore['Player'].apply(lambda x: x.replace(' Jr.', ''))
    train_df = player_df.merge(
        boxscore.drop(columns=['GameID', 'id']), 
        on=['Team', 'Player', 'Date'], 
        how='inner'
        )
    train_df = train_df.merge(opp_df, on=['Opp_Team', 'Date'], how='inner')

    # Append is_back_to_back and home court advantage
    train_df['Home_Court_Advantage'] = train_df.apply(lambda row: 1 if row['Home_Team'] == row['Team'] else 0, axis=1)
    train_df = train_df.drop(columns=['Home_Team'])
    train_df = append_days_since_last_game(train_df)

    return train_df

def back_testing(train_df, features):
   # Sort by date descending
    train_df = train_df.sort_values(by='Date', ascending=False)

    # Get the last 5 days
    last_5_days = train_df['Date'].unique()[:10]

    # Back test for each of the last 5 days
    total_mae = []
    for day in last_5_days:
        # Remove data from the current day onwards
        current_train_set = train_df[train_df['Date'] < day]
        current_test_set = train_df[train_df['Date'] == day]

        # Split the data into training and testing sets
        train_X = current_train_set[features]
        train_Y = current_train_set['Points']
        test_X = current_test_set[features]
        test_Y = current_test_set['Points']

        # Train the model
        model.fit(train_X, train_Y)

        # Predict
        predicted_points = model.predict(test_X)
        mae = mean_absolute_error(test_Y, predicted_points)
        total_mae.append(mae)
    print(f"MAE for each day: {total_mae}")
    print(f"Average MAE: {np.mean(total_mae)}")

def get_feature_importance(model, features):
    # Get feature importance
    feature_importance = model.feature_importances_

    # Get feature names (excluding 'Player' since it was dropped)
    feature_names = [feature for feature in features]

    # Display feature importance
    feature_importance_df = pd.DataFrame({'Feature': feature_names, 'Importance': feature_importance})
    feature_importance_df = feature_importance_df.sort_values(by='Importance', ascending=False)

    # Print the feature importance values
    print(feature_importance_df)

    # Plot the feature importance for better visualization
    plt.figure(figsize=(12, 6))
    plt.barh(feature_importance_df['Feature'], feature_importance_df['Importance'])
    plt.xlabel('Feature Importance')
    plt.title('Model Feature Importance')
    plt.gca().invert_yaxis()
    plt.show()
    feature_importance = model.feature_importances_

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

    # Preprocess the data
    train_df = preprocess_data(player_data, player_tables, opp_data, opp_tables)
    train_df.to_csv('train_df.csv', index=False)

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
    label = 'Points'

    # Prepare feature matrix and target vector
    X = train_df[features]
    Y = train_df[label]

    # Train the model
    model = XGBRegressor(random_state=42, n_estimators=300, max_depth=3, learning_rate=0.1, n_jobs=-1)

    ######## Fit the model ########
    model.fit(X, Y)

    ######## Back Testing Scores ########
    back_testing(train_df, features)

    ######## Save the model ########
    with open('model.pkl', 'wb') as file:
        pickle.dump(model, file)