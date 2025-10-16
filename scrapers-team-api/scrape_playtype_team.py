from nba_api.stats.endpoints import LeagueDashOppPtShot
from datetime import datetime, timedelta, date
import time
import os
import mysql.connector
from contextlib import contextmanager
from dotenv import load_dotenv
import sys
import pandas as pd
from functools import reduce

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
        Opp_2FGM_cns FLOAT,
        Opp_2FGA_cns FLOAT,
        Opp_3PA_cns FLOAT,
        Opp_3PM_cns FLOAT,
        Opp_2FGM_pullup FLOAT,
        Opp_2FGA_pullup FLOAT,
        Opp_3PA_pullup FLOAT,
        Opp_3PM_pullup FLOAT,
        Opp_2FGM_less10 FLOAT,
        Opp_2FGA_less10 FLOAT 
    )
    '''
    cursor.execute(create_table_query)

def insert_data(cursor, row, table_name):
    """Inserts data into the MySQL database """
    insert_query = f'''
    INSERT INTO {table_name} (`Date`, Team, Opp_2FGM_cns, Opp_2FGA_cns, Opp_3PA_cns, Opp_3PM_cns, Opp_2FGM_pullup, 
                            Opp_2FGA_pullup, Opp_3PA_pullup, Opp_3PM_pullup, Opp_2FGM_less10, Opp_2FGA_less10)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

def scrape_data(start_date, end_date, df, playtype):
    # Define the parameters for the API
    params = {
        "general_range_nullable": playtype,
        "per_mode_simple": "PerGame",
        "date_from_nullable": start_date,
        "date_to_nullable": end_date
    }
    # Get the data from the API
    try:
        shot_type = LeagueDashOppPtShot(**params, timeout=10)
    except AttributeError as e:
        print("Encountered an AttributeError:", e)
        return
    except Exception as e:
        print("An error occurred:", e)
        return

    # Parse the data
    shot_type_df = shot_type.get_data_frames()[0]
    shot_type_df = shot_type_df.loc[:, [c for c in shot_type_df.columns 
    if 'PCT' not in str(c) and 'FREQUENCY' not in str(c)]]
    shot_type_df.drop(columns=['TEAM_ID', 'TEAM_NAME', 'GP', 'G', 'FGM', 'FGA'], inplace=True)

    # Mapping for columns names
    cns_mapping = {
        'FG2M': '2FGM_cns',
        'FG2A': '2FGA_cns',
        'FG3A': '3PA_cns',
        'FG3M': '3PM_cns'
    }
    pullup_mapping = {
        'FG2M': '2FGM_pullup',
        'FG2A': '2FGA_pullup',
        'FG3A': '3PA_pullup',
        'FG3M': '3PM_pullup'
    }
    less10_mapping = {
        'FG2M': '2FGM_less10',
        'FG2A': '2FGA_less10'
    }

    # Rename the columns
    if playtype == "Catch and Shoot":
        shot_type_df.columns = [rename_shot_column(c, cns_mapping) for c in shot_type_df.columns]
    elif playtype == "Pullups":
        shot_type_df.columns = [rename_shot_column(c, pullup_mapping) for c in shot_type_df.columns]
    elif playtype == "Less Than 10 ft":
        shot_type_df.columns = [rename_shot_column(c, less10_mapping) for c in shot_type_df.columns]
        shot_type_df.drop(columns=['FG3M', 'FG3A'], inplace=True)
    shot_type_df.rename(columns={'TEAM_ABBREVIATION': 'Team'}, inplace=True)

    # Append the date to the DataFrame as the first col
    formatted_date = datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    shot_type_df.insert(0, 'Date', formatted_date)

    print(f"Scraped Opponent {playtype} data for {formatted_date}")

    # Concatenate the dataframes
    df = pd.concat([df, shot_type_df], ignore_index=True)

    return df

if __name__ == '__main__':
    # Define the number of days to scrape
    target_date = datetime.today().date() - timedelta(days=1)
    last_x_days = 14
    days_to_scrape = (target_date - date(2024, 11, 1)).days

    # Define the playtypes to scrape
    playtypes = ['Catch and Shoot', 'Pullups', 'Less Than 10 ft']
    table_name = 'opp_playtype'
    dataframes = []

    # Iterate through each playtype
    for playtype in playtypes:
        df = pd.DataFrame()

        # Determine dates to scrape
        with connect_to_sql() as (cursor, conn):
            create_table(cursor, table_name)
            dates_to_scrape = get_dates_to_scrape(cursor, table_name, target_date, days_to_scrape)
            if not dates_to_scrape:
                sys.exit(print("No new data to scrape."))

        # Scrape the data
        for start_date, end_date in get_date_range(dates_to_scrape, last_x_days):
            df = scrape_data(start_date, end_date, df, playtype)
            time.sleep(0.6)
        dataframes.append(df)

    # Merge and export the dataframes
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=['Date', 'Team'], how='inner'), dataframes)
    export_data_to_sql(merged_df, table_name)
