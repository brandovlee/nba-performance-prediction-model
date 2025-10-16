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
        PTS_OFF_TOV FLOAT,
        PTS_2ND_CHANCE FLOAT,
        PTS_FB FLOAT,
        PTS_PAINT FLOAT,
        PTS_OFF_TOV_RANK FLOAT,
        PTS_2ND_CHANCE_RANK FLOAT,
        PTS_FB_RANK FLOAT,
        PTS_PAINT_RANK FLOAT
    )
    '''
    cursor.execute(create_table_query)

def insert_data(cursor, row, table_name):
    """Inserts data into the MySQL database """
    data = ['Date', 'Player', 'TEAM', 'PTS_OFF_TOV', 'PTS_2ND_CHANCE', 'PTS_FB',
       'PTS_PAINT', 'PTS_OFF_TOV_RANK', 'PTS_2ND_CHANCE_RANK', 'PTS_FB_RANK',
       'PTS_PAINT_RANK']

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
        "measure_type_detailed_defense": "Misc",
        "per_mode_detailed": "PerGame",
        "date_from_nullable": start_date,
        "date_to_nullable": end_date
    }
    # Get the data from the API
    try:
        misc = LeagueDashPlayerStats(**params, timeout=10)
    except AttributeError as e:
        print("Encountered an AttributeError:", e)
        return
    except Exception as e:
        print("An error occurred:", e)
        return
    # Parse the data
    misc_df = misc.get_data_frames()[0]
    misc_df.drop(columns=['TEAM_ID', 'PLAYER_ID', 'NICKNAME', 'AGE', 'GP', 'W', 'L', 'W_PCT',
                'MIN', 'OPP_PTS_OFF_TOV', 'OPP_PTS_2ND_CHANCE', 'OPP_PTS_FB', 'OPP_PTS_PAINT', 
                'BLK', 'BLKA', 'PF', 'PFD', 'NBA_FANTASY_PTS', 'GP_RANK', 'W_RANK', 'L_RANK', 'W_PCT_RANK',
                'MIN_RANK', 'OPP_PTS_OFF_TOV_RANK', 'OPP_PTS_2ND_CHANCE_RANK', 'OPP_PTS_FB_RANK', 
                'OPP_PTS_PAINT_RANK', 'BLK_RANK', 'BLKA_RANK', 'PF_RANK', 'PFD_RANK', 
                'NBA_FANTASY_PTS_RANK'], inplace=True)

    # Mapping for columns names
    mapping = {
        'PLAYER_NAME': 'Player', 'TEAM_ABBREVIATION': 'TEAM'}

    # Rename the columns
    misc_df.columns = [rename_column(c, mapping) for c in misc_df.columns]

    # Remove inconsistent formatting from player names
    misc_df['Player'] = misc_df['Player'].apply(lambda x: x.replace(' Jr.', ''))
    misc_df['Player'] = misc_df['Player'].apply(lambda x: remove_diacritics(x))

    # Append the date to the DataFrame as the first col
    formatted_date = datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    misc_df.insert(0, 'Date', formatted_date)

    # Fill NaN values with 0
    misc_df.fillna(0, inplace=True)

    # Concat the dataframes
    df = pd.concat([df, misc_df], ignore_index=True)
    print(f"Scraped Player Misc data for {formatted_date}")
    
    return df

if __name__ == '__main__':
    # Define the table name
    table_name = 'player_misc'

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