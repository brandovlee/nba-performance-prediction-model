from nba_api.stats.endpoints import LeagueDashTeamShotLocations
from datetime import datetime, timedelta, date
import os
import mysql.connector
from contextlib import contextmanager
from dotenv import load_dotenv
import sys
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
        Team varchar(255),
        Opp_RA_FGM FLOAT,
        Opp_RA_FGA FLOAT,
        Opp_Paint_FGM FLOAT,
        Opp_Paint_FGA FLOAT,
        Opp_Mid_FGM FLOAT,
        Opp_Mid_FGA FLOAT,
        Opp_LC3_FGM FLOAT,
        Opp_LC3_FGA FLOAT,
        Opp_RC3_FGM FLOAT,
        Opp_RC3_FGA FLOAT,
        Opp_AB3_FGM FLOAT,
        Opp_AB3_FGA FLOAT,
        Opp_C3_FGM FLOAT,
        Opp_C3_FGA FLOAT
    )
    '''
    cursor.execute(create_table_query)

def insert_data(cursor, row, table_name):
    """Inserts data into the MySQL database """
    insert_query = f'''
    INSERT INTO {table_name} (`Date`, Team, Opp_RA_FGM, Opp_RA_FGA, Opp_Paint_FGM,
                            Opp_Paint_FGA, Opp_Mid_FGM, Opp_Mid_FGA, Opp_LC3_FGM,
                            Opp_LC3_FGA, Opp_RC3_FGM, Opp_RC3_FGA, Opp_AB3_FGM,
                            Opp_AB3_FGA, Opp_C3_FGM, Opp_C3_FGA)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    for original, short_label in mapping.items():
        if original in col_name:
            col_name = col_name.replace(original, short_label)
    return col_name

def scrape_data(start_date, end_date, df):
    # Define the parameters for the API
    params = {
        "distance_range": "By Zone",
        "per_mode_detailed": "PerGame",
        "measure_type_simple": "Opponent",
        "date_from_nullable": start_date,
        "date_to_nullable": end_date
    }
    # Get the data from the API
    try:
        shot_locations = LeagueDashTeamShotLocations(**params, timeout=10)
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

    # Rename the columns
    shot_locations_df = shot_locations_df.loc[:, [c for c in shot_locations_df.columns 
    if 'PCT' not in str(c) and 'Backcourt' not in str(c)]]
    shot_locations_df = shot_locations_df.drop(columns=['TEAM_ID'])


    # Mapping for columns names
    zone_mapping = {
        'Restricted Area_OPP_FGM': 'Opp_RA_FGM',
        'In The Paint (Non-RA)_OPP_FGM': 'Opp_Paint_FGM',
        'Mid-Range_OPP_FGM': 'Opp_Mid_FGM',
        'Left Corner 3_OPP_FGM': 'Opp_LC3_FGM',
        'Right Corner 3_OPP_FGM': 'Opp_RC3_FGM',
        'Above the Break 3_OPP_FGM': 'Opp_AB3_FGM',
        'Corner 3_OPP_FGM': 'Opp_C3_FGM',
        'Restricted Area_OPP_FGA': 'Opp_RA_FGA',
        'In The Paint (Non-RA)_OPP_FGA': 'Opp_Paint_FGA',
        'Mid-Range_OPP_FGA': 'Opp_Mid_FGA',
        'Left Corner 3_OPP_FGA': 'Opp_LC3_FGA',
        'Right Corner 3_OPP_FGA': 'Opp_RC3_FGA',
        'Above the Break 3_OPP_FGA': 'Opp_AB3_FGA',
        'Corner 3_OPP_FGA': 'Opp_C3_FGA'
    }
    # Mapping for team names
    team_mapping = {
    'Utah Jazz': 'UTA', 'Chicago Bulls': 'CHI', 'Phoenix Suns': 'PHX',
    'Golden State Warriors': 'GSW', 'Charlotte Hornets': 'CHA', 'Miami Heat': 'MIA',
    'Memphis Grizzlies': 'MEM', 'Dallas Mavericks': 'DAL', 'New Orleans Pelicans': 'NOP',
    'Oklahoma City Thunder': 'OKC', 'Los Angeles Lakers': 'LAL', 'Toronto Raptors': 'TOR',
    'Atlanta Hawks': 'ATL', 'Milwaukee Bucks': 'MIL', 'Washington Wizards': 'WAS',
    'Sacramento Kings': 'SAC', 'Detroit Pistons': 'DET', 'Philadelphia 76ers': 'PHI',
    'New York Knicks': 'NYK', 'LA Clippers': 'LAC', 'Cleveland Cavaliers': 'CLE',
    'Houston Rockets': 'HOU', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BKN',
    'Denver Nuggets': 'DEN', 'Orlando Magic': 'ORL', 'Portland Trail Blazers': 'POR',
    'Indiana Pacers': 'IND', 'San Antonio Spurs': 'SAS', 'Minnesota Timberwolves': 'MIN'
    }
    

    # Rename the columns
    shot_locations_df.columns = [rename_shot_column(c, zone_mapping) for c in shot_locations_df.columns]
    shot_locations_df['TEAM_NAME'] = shot_locations_df['TEAM_NAME'].map(team_mapping)
    shot_locations_df.rename(columns={'TEAM_NAME': 'Team'}, inplace=True)

    # Append the date to the DataFrame as the first col
    formatted_date = datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    shot_locations_df.insert(0, 'Date', formatted_date)

    # Concat the dataframes
    df = pd.concat([df, shot_locations_df], ignore_index=True)
    print(f"Scraped Opponent Shot Location data for {formatted_date}")

    return df

if __name__ == '__main__':
    # Define the table name
    table_name = 'opp_shot_locations'

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
    