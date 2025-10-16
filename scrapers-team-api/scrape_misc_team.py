from nba_api.stats.endpoints import LeagueDashTeamStats
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
        OPP_PTS_OFF_TOV FLOAT, 
        OPP_PTS_2ND_CHANCE FLOAT, 
        OPP_PTS_FB FLOAT,
        OPP_PTS_PAINT FLOAT
    )
    '''
    cursor.execute(create_table_query)

def insert_data(cursor, row, table_name):
    """Inserts data into the MySQL database """
    data = ['Date', 'Team', 'OPP_PTS_OFF_TOV', 'OPP_PTS_2ND_CHANCE', 'OPP_PTS_FB', 'OPP_PTS_PAINT']
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

def scrape_data(start_date, end_date, df):
    # Define the parameters for the API
    params = {
        "per_mode_detailed": "PerGame",
        "measure_type_detailed_defense": "Misc",
        "date_from_nullable": start_date,
        "date_to_nullable": end_date
    }
    # Get the data from the API
    try:
        misc = LeagueDashTeamStats(**params, timeout=10)
    except AttributeError as e:
        print("Encountered an AttributeError:", e)
        return
    except Exception as e:
        print("An error occurred:", e)
        return

    # Parse the data
    misc_df = misc.get_data_frames()[0]
    misc_df = misc_df.drop(columns=['TEAM_ID', 'GP', 'W', 'L', 'W_PCT', 'MIN', 'PTS_OFF_TOV',
                        'PTS_2ND_CHANCE', 'PTS_FB', 'PTS_PAINT', 'GP_RANK', 'W_RANK', 
                        'L_RANK', 'W_PCT_RANK', 'MIN_RANK', 'PTS_OFF_TOV_RANK',
                        'PTS_2ND_CHANCE_RANK', 'PTS_FB_RANK', 'PTS_PAINT_RANK',
                        'OPP_PTS_OFF_TOV_RANK', 'OPP_PTS_2ND_CHANCE_RANK', 'OPP_PTS_FB_RANK',
                        'OPP_PTS_PAINT_RANK'])

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
    misc_df['TEAM_NAME'] = misc_df['TEAM_NAME'].map(team_mapping)
    misc_df.rename(columns={'TEAM_NAME': 'Team'}, inplace=True)

    # Append the date to the DataFrame as the first col
    formatted_date = datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    misc_df.insert(0, 'Date', formatted_date)

    # Concat the dataframes
    df = pd.concat([df, misc_df], ignore_index=True)
    print(f"Scraped Opponent Misc data for {formatted_date}")

    return df

if __name__ == '__main__':
    # Define the table name
    table_name = 'opp_misc'

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
    