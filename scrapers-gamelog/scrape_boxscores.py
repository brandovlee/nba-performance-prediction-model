from nba_api.stats.endpoints import boxscoretraditionalv3
from nba_api.stats.endpoints import ScoreboardV2
from nba_api.stats.static import teams
from datetime import datetime, timedelta
import os
import time
import mysql.connector
from contextlib import contextmanager
from dotenv import load_dotenv
import unicodedata

@contextmanager
def connect_to_sql():
    """Connects to the SQL using contextmanager to efficiently manage the connection and cursor"""
    conn = None
    cursor = None  # Initialize cursor to None
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
        raise  # Re-raise the exception to handle it outside
    finally:
        # Close cursor and conn after usage
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
        `GameID` INT,
        `Date` DATE,
        `Home_Team` VARCHAR(255),
        `Team` VARCHAR(255),
        `Player` VARCHAR(255),
        `Opp_Team` VARCHAR(255),
        `Points` FLOAT,
        `Minutes` FLOAT
    )
    '''
    cursor.execute(create_table_query)

def insert_data(cursor, data, table_name):
    """Inserts data into the MySQL database """
    insert_query = f'''
    INSERT INTO `{table_name}` (`GameID`, `Date`, `Home_Team`, `Team`, `Player`, `Opp_Team`, `Points`, `Minutes`) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''
    cursor.executemany(insert_query, data)

def export_data_to_sql(data, table_name):
    """Exports the data to the MySQL database """
    with connect_to_sql() as (cursor, conn):
        # Create table
        create_table(cursor, table_name)

        # Check if player names match NBA.com
        check_names(cursor, data)

        # Insert data
        data_to_insert = [(item['GameID'], item['Date'], item['Home_Team'], item['Team'], item['Player'], 
                           item['Opp_Team'], item['Points'], item['Minutes']) 
                          for item in data]
        insert_data(cursor, data_to_insert, table_name)
        conn.commit()

def manage_table(cursor, table_name, target_date, days_to_scrape):
    # Define the query to remove data older than X days
    query_remove = f"""
    DELETE FROM {table_name}
    WHERE Date < DATE_SUB('{target_date}', INTERVAL {days_to_scrape} DAY);
    """

    # Define the query to find distinct dates scraped
    query_find_dates_scraped = f"""
    SELECT DISTINCT Date
    FROM {table_name}
    """

    # Execute the query to remove data older than X days
    # cursor.execute(query_remove)

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

def remove_diacritics(input_str):
    # Normalize the string to decompose characters with diacritics
    normalized = unicodedata.normalize('NFD', input_str)
    # Remove diacritics by filtering out characters in the 'Mn' category (mark, nonspacing)
    without_diacritics = ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')
    return without_diacritics

def check_names(cursor, data):
    try:
        # Query to check if player names are in the MySQL database
        query = "SELECT Player FROM player_traditional"
        cursor.execute(query)
        result = cursor.fetchall()

        # Store the player names in a set
        player_names = set()
        for player in result:
            player_names.add(player[0])
        
        # Iterate through the player names in the data
        for item in data:
            if item['Player'] not in player_names:
                print(f"WARNING: {item['Player']} not found on NBA.com")

    except Exception as e:
        print("An error occurred:", e)
        # Exit the script upon encountering an error
        exit()

def scrape_game_data(game_date):
    def get_team_name(team_id):
        team_info = teams.find_team_name_by_id(team_id)
        return team_info.get('abbreviation') if team_info else None
    
    params = {
        "game_date": game_date,
        "league_id": "00",
        "day_offset": "0"
    }

    games = ScoreboardV2(**params, timeout=30)
    games_dict = games.get_dict()
    result_sets = games_dict.get('resultSets')[0].get('rowSet')

    game_data = []
    for result in result_sets:
        _, team_names = result[5].split('/')
        team_names_formatted = [team_names[:3], team_names[3:]]
        game_id = result[2]
        home_team = result[6]
        home_team_abbr = get_team_name(home_team)
        if not home_team_abbr: # Skip if the team abbreviation is not found (e.g. All-Star Game)
            continue
        game_data.append({
            'game_data': game_date,
            'game_id': game_id, 
            'team_names': team_names_formatted,
            'home_team': home_team_abbr
            })
    
    return game_data

def scrape_box_score(data):
    def convert_time_to_minutes(time_str):
        try:
            time_str = time_str.strip()
            if ':' in time_str:
                minutes, seconds = time_str.split(':')
                return int(minutes) + int(seconds) / 60
            else:
                return float(time_str)
        except ValueError:
            return 0.0
    
    game_id = data['game_id']
    game_date = data['game_data']
    home_team = data['home_team']
    params = {
        "game_id": game_id,
        "start_period": 1,
        "end_period": 1,
        "start_range": 0,
        "end_range": 0,
        "range_type": 0
    }

    # Error handling for the API call
    try:
        boxscore = boxscoretraditionalv3.BoxScoreTraditionalV3(**params, timeout=10)
    except AttributeError as e:
        print("Encountered an AttributeError:", e)
        return
    except Exception as e:
        print("An error occurred:", e)
        return

    # Continue if there's a response
    player_stats = boxscore.get_data_frames()[0]
    output_data = []

    for player in player_stats.iterrows():
        minutes = player[1]['minutes']
        total_minutes = convert_time_to_minutes(minutes)
        if total_minutes:
            player_name = (player[1]['firstName'] + ' ' + player[1]['familyName'])
            player_name = remove_diacritics(player_name) # Remove diacritics from player name
            player_name = player_name.replace(' Jr.', '') # Remove 'Jr.' from player name
            points = player[1]['points']
            team_name = player[1]['teamTricode']

            opp_team = next(team for team in data['team_names'] if team != team_name)

            output_data.append({'GameID': int(game_id),
                                'Date': game_date,
                                'Home_Team': home_team,
                                'Team': team_name,
                                'Player': player_name, 
                                'Opp_Team': opp_team,
                                'Points': points,
                                'Minutes': total_minutes})
    return output_data

def is_boxscore_already_in_db(cursor, table_name, game_id):
    query = f"""
    SELECT COUNT(*)
    FROM {table_name}
    WHERE GameID = '{game_id}';
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] > 0

def main():
    game_data = []
    todays_date = datetime.today().date()  # get today's date
    target_date = todays_date - timedelta(days=1) # subtract 1 day to get yesterday's date
    days_to_scrape = 65 # retrieve data for the last 14 days

    # Remove data older than X days and get the latest date
    with connect_to_sql() as (cursor, conn):
        # Create the table
        create_table(cursor, 'player_boxscore')
        dates_to_scrape = manage_table(cursor, 'player_boxscore', target_date, days_to_scrape)
        if not dates_to_scrape:
            print("No new data to scrape.")
            return

    # Scrape game data for the last X days
    for date in dates_to_scrape:
        data = scrape_game_data(date)
        if not data:
            with connect_to_sql() as (cursor, conn):
                insert_query = f'''
                INSERT INTO `{'player_boxscore'}` (`GameID`, `Date`, `Home_Team`, `Team`, `Player`, `Opp_Team`, `Points`, `Minutes`) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(insert_query, [0, date, 0, "n/a", "n/a", "n/a", 0, 0])
                conn.commit()
        else:
            game_data += data
        time.sleep(0.7)
        print(f"Scraped game data for {date}")

    # Remove games that are already in the database
    game_data_copy = game_data.copy()
    with connect_to_sql() as (cursor, conn):
        for data in game_data_copy:
            if is_boxscore_already_in_db(cursor, 'player_boxscore', data['game_id']):
                game_data.remove(data)

    # Scrape box scores for the games
    output_data = []
    for data in game_data:
        boxscore_data = scrape_box_score(data)
        if boxscore_data:
            output_data += boxscore_data
        time.sleep(0.7)
        print(f"Scraped box score for {data['game_data']}")
    
    # Export data to SQL if new data was scraped
    if output_data:
        export_data_to_sql(output_data, 'player_boxscore')

if __name__ == '__main__':
    main()

