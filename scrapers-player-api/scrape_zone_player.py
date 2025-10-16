from nba_api.stats.endpoints import LeagueDashPlayerShotLocations
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
        RA_FGM FLOAT,
        RA_FGA FLOAT,
        Paint_FGM FLOAT,
        Paint_FGA FLOAT,
        Mid_FGM FLOAT,
        Mid_FGA FLOAT,
        LC3_FGM FLOAT,
        LC3_FGA FLOAT,
        RC3_FGM FLOAT,
        RC3_FGA FLOAT,
        AB3_FGM FLOAT,
        AB3_FGA FLOAT,
        C3_FGM FLOAT,
        C3_FGA FLOAT
    )
    '''
    cursor.execute(create_table_query)

def insert_data(cursor, row, table_name):
    """Inserts data into the MySQL database """
    insert_query = f'''
    INSERT INTO {table_name} (`Date`, Player, Team, RA_FGM, RA_FGA, Paint_FGM,
                            Paint_FGA, Mid_FGM, Mid_FGA, LC3_FGM,
                            LC3_FGA, RC3_FGM, RC3_FGA, AB3_FGM,
                            AB3_FGA, C3_FGM, C3_FGA)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

def rename_shot_column(col_name, mapping):
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
        "distance_range": "By Zone",
        "per_mode_detailed": "PerGame",
        "date_from_nullable": start_date,
        "date_to_nullable": end_date
    }
    # Get the data from the API
    try:
        shot_locations = LeagueDashPlayerShotLocations(**params, timeout=10)
    except AttributeError as e:
        print("Encountered an AttributeError:", e)
        return
    except Exception as e:
        print("An error occurred:", e)
        return

    # Parse the data
    shot_locations_df = shot_locations.get_data_frames()[0]

    # Flatten the MultiIndex
    shot_locations_df.columns = [
        '_'.join(str(x) for x in col if x)
        for col in shot_locations_df.columns
    ]

    # Mapping for columns names
    zone_mapping = {
        'Restricted Area_FGM': 'RA_FGM',
        'In The Paint (Non-RA)_FGM': 'Paint_FGM',
        'Mid-Range_FGM': 'Mid_FGM',
        'Left Corner 3_FGM': 'LC3_FGM',
        'Right Corner 3_FGM': 'RC3_FGM',
        'Above the Break 3_FGM': 'AB3_FGM',
        'Corner 3_FGM': 'C3_FGM',
        'Restricted Area_FGA': 'RA_FGA',
        'In The Paint (Non-RA)_FGA': 'Paint_FGA',
        'Mid-Range_FGA': 'Mid_FGA',
        'Left Corner 3_FGA': 'LC3_FGA',
        'Right Corner 3_FGA': 'RC3_FGA',
        'Above the Break 3_FGA': 'AB3_FGA',
        'Corner 3_FGA': 'C3_FGA'
    }
    
    # Remove unused columns
    shot_locations_df = shot_locations_df.loc[:, [c for c in shot_locations_df.columns 
    if 'PCT' not in str(c) and 'Backcourt' not in str(c)]]
    shot_locations_df = shot_locations_df.drop(columns=['TEAM_ID', 'PLAYER_ID', 'AGE', 'NICKNAME'])

    # Rename the columns
    shot_locations_df.columns = [rename_shot_column(c, zone_mapping) for c in shot_locations_df.columns]
    shot_locations_df.rename(columns={'TEAM_ABBREVIATION': 'Team'}, inplace=True)
    shot_locations_df.rename(columns={'PLAYER_NAME': 'Player'}, inplace=True)

    # Remove inconsistent formatting from player names
    shot_locations_df['Player'] = shot_locations_df['Player'].apply(lambda x: x.replace(' Jr.', ''))
    shot_locations_df['Player'] = shot_locations_df['Player'].apply(lambda x: remove_diacritics(x))

    # Append the date to the DataFrame as the first col
    formatted_date = datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    shot_locations_df.insert(0, 'Date', formatted_date)

    # Fill NaN values with 0
    shot_locations_df.fillna(0, inplace=True)

    # Concat the dataframes
    df = pd.concat([df, shot_locations_df], ignore_index=True)
    print(f"Scraped Player Shot Location data for {formatted_date}")
    
    return df

if __name__ == '__main__':
    # Define the table name
    table_name = 'player_shot_locations'

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
    