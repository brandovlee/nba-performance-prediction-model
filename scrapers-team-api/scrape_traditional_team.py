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
        OPP_FGM FLOAT,
        OPP_FGA FLOAT,
        OPP_FG_PCT FLOAT,
        OPP_3PM FLOAT,
        OPP_3PA FLOAT,
        OPP_3P_PCT FLOAT,
        OPP_FTM FLOAT,
        OPP_FTA FLOAT,
        OPP_FT_PCT FLOAT,
        OPP_OREB FLOAT,
        OPP_DREB FLOAT,
        OPP_REB FLOAT,
        OPP_AST FLOAT,
        OPP_TOV FLOAT,
        OPP_STL FLOAT,
        OPP_BLK FLOAT,
        OPP_BLKA FLOAT,
        OPP_PF FLOAT,
        OPP_PFD FLOAT,
        OPP_PTS FLOAT,
        PLUS_MINUS FLOAT,
        OPP_FGM_RANK FLOAT,
        OPP_FGA_RANK FLOAT,
        OPP_FG_PCT_RANK FLOAT,
        OPP_3PM_RANK FLOAT,
        OPP_3PA_RANK FLOAT,
        OPP_3P_PCT_RANK FLOAT,
        OPP_FTM_RANK FLOAT,
        OPP_FTA_RANK FLOAT,
        OPP_FT_PCT_RANK FLOAT,
        OPP_OREB_RANK FLOAT,
        OPP_DREB_RANK FLOAT,
        OPP_REB_RANK FLOAT,
        OPP_AST_RANK FLOAT,
        OPP_TOV_RANK FLOAT,
        OPP_STL_RANK FLOAT,
        OPP_BLK_RANK FLOAT,
        OPP_BLKA_RANK FLOAT,
        OPP_PF_RANK FLOAT,
        OPP_PFD_RANK FLOAT,
        OPP_PTS_RANK FLOAT,
        PLUS_MINUS_RANK FLOAT
    )
    '''
    cursor.execute(create_table_query)

def insert_data(cursor, row, table_name):
    """Inserts data into the MySQL database """
    data = ['Date', 'Team', 'OPP_FGM', 'OPP_FGA', 'OPP_FG_PCT', 'OPP_3PM',
       'OPP_3PA', 'OPP_3P_PCT', 'OPP_FTM', 'OPP_FTA', 'OPP_FT_PCT', 'OPP_OREB',
       'OPP_DREB', 'OPP_REB', 'OPP_AST', 'OPP_TOV', 'OPP_STL', 'OPP_BLK',
       'OPP_BLKA', 'OPP_PF', 'OPP_PFD', 'OPP_PTS', 'PLUS_MINUS',
       'OPP_FGM_RANK', 'OPP_FGA_RANK', 'OPP_FG_PCT_RANK', 'OPP_3PM_RANK',
       'OPP_3PA_RANK', 'OPP_3P_PCT_RANK', 'OPP_FTM_RANK', 'OPP_FTA_RANK',
       'OPP_FT_PCT_RANK', 'OPP_OREB_RANK', 'OPP_DREB_RANK', 'OPP_REB_RANK',
       'OPP_AST_RANK', 'OPP_TOV_RANK', 'OPP_STL_RANK', 'OPP_BLK_RANK',
       'OPP_BLKA_RANK', 'OPP_PF_RANK', 'OPP_PFD_RANK', 'OPP_PTS_RANK',
       'PLUS_MINUS_RANK']
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
    for original, short_label in mapping.items():
        if original in col_name:
            col_name = col_name.replace(original, short_label)
    return col_name

def scrape_data(start_date, end_date, df):
    # Define the parameters for the API
    params = {
        "per_mode_detailed": "PerGame",
        "measure_type_detailed_defense": "Opponent",
        "date_from_nullable": start_date,
        "date_to_nullable": end_date
    }
    # Get the data from the API
    try:
        traditional = LeagueDashTeamStats(**params, timeout=10)
    except AttributeError as e:
        print("Encountered an AttributeError:", e)
        return
    except Exception as e:
        print("An error occurred:", e)
        return

    # Parse the data
    traditional_df = traditional.get_data_frames()[0]
    traditional_df = traditional_df.drop(columns=['TEAM_ID', 'GP', 'W', 'L', 'W_PCT', 'MIN', 'GP_RANK',
                                        'W_RANK', 'L_RANK', 'W_PCT_RANK', 'MIN_RANK'])

    # Mapping for columns
    mapping = {
        'OPP_FG3M': 'OPP_3PM', 'OPP_FG3A': 'OPP_3PA', 'OPP_FG3_PCT': 'OPP_3P_PCT'
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
    traditional_df.columns = [rename_column(col, mapping) for col in traditional_df.columns]
    traditional_df['TEAM_NAME'] = traditional_df['TEAM_NAME'].map(team_mapping)
    traditional_df.rename(columns={'TEAM_NAME': 'Team'}, inplace=True)

    # Append the date to the DataFrame as the first col
    formatted_date = datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    traditional_df.insert(0, 'Date', formatted_date)

    # Concat the dataframes
    df = pd.concat([df, traditional_df], ignore_index=True)
    print(f"Scraped Opponent traditional data for {formatted_date}")

    return df

if __name__ == '__main__':
    # Define the table name
    table_name = 'opp_traditional'

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
    