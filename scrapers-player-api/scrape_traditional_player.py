from nba_api.stats.endpoints import LeagueDashPlayerStats
from datetime import datetime, timedelta, date
import os
import mysql.connector
from contextlib import contextmanager
from dotenv import load_dotenv
import sys
import unicodedata
import pandas as pd
import time

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
    finally: # close cursor and conn after usage
        if cursor:
            cursor.close()
        if conn:
            conn.close() 

def create_table(cursor, table_name):
    """Creates a new table if it does not exist in the MySQL database"""
    # Define the query to create the table
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        `Date` DATE,
        Player varchar(255),
        Team varchar(255),
        AGE FLOAT,
        GP FLOAT,
        W FLOAT,
        L FLOAT,
        W_PCT FLOAT,
        MIN FLOAT,
        FGM FLOAT,
        FGA FLOAT,
        FG_PCT FLOAT,
        3PM FLOAT,
        3PA FLOAT,
        3P_PCT FLOAT,
        FTM FLOAT,
        FTA FLOAT,
        FT_PCT FLOAT,
        OREB FLOAT,
        DREB FLOAT,
        REB FLOAT,
        AST FLOAT,
        TOV FLOAT,
        STL FLOAT,
        BLK FLOAT,
        BLKA FLOAT,
        PF FLOAT,
        PFD FLOAT,
        PPG FLOAT,
        PLUS_MINUS FLOAT,
        NBA_FANTASY_PPG FLOAT,
        DD2 FLOAT,
        TD3 FLOAT,
        GP_RANK FLOAT,
        W_RANK FLOAT,
        L_RANK FLOAT,
        W_PCT_RANK FLOAT,
        MIN_RANK FLOAT,
        FGM_RANK FLOAT,
        FGA_RANK FLOAT,
        FG_PCT_RANK FLOAT,
        3PM_RANK FLOAT,
        3PA_RANK FLOAT,
        3P_PCT_RANK FLOAT,
        FTM_RANK FLOAT,
        FTA_RANK FLOAT,
        FT_PCT_RANK FLOAT,
        OREB_RANK FLOAT,
        DREB_RANK FLOAT,
        REB_RANK FLOAT,
        AST_RANK FLOAT,
        TOV_RANK FLOAT,
        STL_RANK FLOAT,
        BLK_RANK FLOAT,
        BLKA_RANK FLOAT,
        PF_RANK FLOAT,
        PFD_RANK FLOAT,
        PPG_RANK FLOAT,
        PLUS_MINUS_RANK FLOAT,
        NBA_FANTASY_PPG_RANK FLOAT,
        DD2_RANK FLOAT,
        TD3_RANK FLOAT
    )
    '''
    cursor.execute(create_table_query)

def insert_data(cursor, row, table_name):
    """Inserts data into the MySQL database """
    data = ['Date', 'Player', 'TEAM', 'AGE', 'GP', 'W', 'L', 'W_PCT', 'MIN', 'FGM',
       'FGA', 'FG_PCT', '3PM', '3PA', '3P_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
       'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PPG',
       'PLUS_MINUS', 'NBA_FANTASY_PPG', 'DD2', 'TD3', 'GP_RANK', 'W_RANK',
       'L_RANK', 'W_PCT_RANK', 'MIN_RANK', 'FGM_RANK', 'FGA_RANK',
       'FG_PCT_RANK', '3PM_RANK', '3PA_RANK', '3P_PCT_RANK', 'FTM_RANK',
       'FTA_RANK', 'FT_PCT_RANK', 'OREB_RANK', 'DREB_RANK', 'REB_RANK',
       'AST_RANK', 'TOV_RANK', 'STL_RANK', 'BLK_RANK', 'BLKA_RANK', 'PF_RANK',
       'PFD_RANK', 'PPG_RANK', 'PLUS_MINUS_RANK', 'NBA_FANTASY_PPG_RANK',
       'DD2_RANK', 'TD3_RANK']

    insert_query = f'''
    INSERT INTO {table_name} ({', '.join(data)})
    VALUES ({', '.join(['%s'] * len(data))})
    '''
    cursor.execute(insert_query, tuple(row))

def export_data_to_sql(data, table_name):
    """Exports the data to the MySQL database """
    with connect_to_sql() as (cursor, conn):
        for _, row in data.iterrows():
            insert_data(cursor, row, table_name)
        conn.commit()

def get_dates_to_scrape(cursor, table_name, target_date, days_to_scrape):
    # Define the query to find distinct dates scraped
    query_find_dates_scraped = f"""
    SELECT DISTINCT Date
    FROM {table_name}
    """

    # Execute the query to find the dates scraped
    cursor.execute(query_find_dates_scraped)
    unique_dates = cursor.fetchall()
    dates_scraped = set(date[0] for date in unique_dates)

    # Generate list of dates
    date_range = [
        (target_date - timedelta(days=i))
        for i in range(days_to_scrape + 1)
    ]

    dates_to_scrape = list(set(date_range) - dates_scraped)

    return sorted(dates_to_scrape)[::-1]

def get_date_range(dates_to_scrape, last_x_days):    
    # Create a tuple of start and end dates for a X day window
    date_range = []
    for date in dates_to_scrape:
        start_date = (date - timedelta(days=last_x_days)).strftime('%m/%d/%Y')
        date_range.append((start_date, date.strftime('%m/%d/%Y')))
    
    return date_range

def rename_column(col_name, mapping):
    # For each zone in mapping, replace the zone text with the short label
    for original, short_label in mapping.items():
        if original in col_name:
            col_name = col_name.replace(original, short_label)
    return col_name

def remove_diacritics(input_str):
    # Normalize the string to decompose characters with diacritics
    normalized = unicodedata.normalize('NFD', input_str)
    # Remove diacritics by filtering out characters in the 'Mn' category (mark, nonspacing)
    without_diacritics = ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')
    return without_diacritics

def scrape_data(start_date, end_date, df):
    # Define the parameters for the API
    params = {
        "per_mode_detailed": "PerGame",
        "date_from_nullable": start_date,
        "date_to_nullable": end_date
    }
    # Get the data from the API
    try:
        traditional = LeagueDashPlayerStats(**params, timeout=10)
    except AttributeError as e:
        print("Encountered an AttributeError:", e)
        return
    except Exception as e:
        print("An error occurred:", e)
        return

    # Parse the data
    traditional_df = traditional.get_data_frames()[0]
    traditional_df.drop(columns=['TEAM_ID', 'PLAYER_ID', 'NICKNAME', 
                                'WNBA_FANTASY_PTS', 'WNBA_FANTASY_PTS_RANK'], inplace=True)

    # Mapping for columns names
    mapping = {
        'PLAYER_NAME': 'Player', 'TEAM_ABBREVIATION': 'TEAM',
        'FG3M': '3PM', 'FG3A': '3PA', 'FG3_PCT': '3P_PCT',
        'PTS': 'PPG'}

    # Rename the columns
    traditional_df.columns = [rename_column(c, mapping) for c in traditional_df.columns]

    # Remove inconsistent formatting from player names
    traditional_df['Player'] = traditional_df['Player'].apply(lambda x: x.replace(' Jr.', ''))
    traditional_df['Player'] = traditional_df['Player'].apply(lambda x: remove_diacritics(x))

    # Append the date to the DataFrame as the first col
    formatted_date = datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    traditional_df.insert(0, 'Date', formatted_date)

    # Fill NaN values with 0
    traditional_df.fillna(0, inplace=True)

    # Concat the dataframes
    df = pd.concat([df, traditional_df], ignore_index=True)
    print(f"Scraped Player Traditional data for {formatted_date}")
    
    return df

if __name__ == '__main__':
    # Define the table name
    table_name = 'player_traditional'

    # Define the number of days to scrape
    target_date = datetime.today().date() - timedelta(days=1)
    last_x_days = 14
    days_to_scrape = (target_date - date(2024, 11, 1)).days
    with connect_to_sql() as (cursor, conn):
        create_table(cursor, table_name)
        dates_to_scrape = get_dates_to_scrape(cursor, table_name, target_date, days_to_scrape)
        if not dates_to_scrape:
            sys.exit(print("No new data to scrape."))
    
    # Scrape and export the data
    df = pd.DataFrame()
    for start_date, end_date in get_date_range(dates_to_scrape, last_x_days):
        df = scrape_data(start_date, end_date, df)
        time.sleep(0.6)
    export_data_to_sql(df, table_name)