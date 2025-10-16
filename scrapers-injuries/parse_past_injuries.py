import mysql.connector
import os
from contextlib import contextmanager
from dotenv import load_dotenv
from datetime import date
from contextlib import contextmanager

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
    #  Drop the table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Define the query to create the table
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        `Date` DATE,
        `Team` VARCHAR(255),
        `Player` VARCHAR(255)
    )
    '''
    cursor.execute(create_table_query)

def export_data_to_sql(data, table_name):
    """Exports the data to the MySQL database"""
    with connect_to_sql() as (cursor, conn):
        # Create the table if it does not exist
        create_table(cursor, table_name)

        # Insert the data into the table
        for row in data:
            cursor.execute(f"INSERT INTO {table_name} (`Date`, `Team`, `Player`) VALUES (%s, %s, %s)", row)
        conn.commit()

def fetch_all_players():
    with connect_to_sql() as (cursor, conn):
        cursor.execute("SELECT DISTINCT team, player FROM player_traditional")
        unique_players = cursor.fetchall()
    
    # Create a dictionary to store the players by team
    players_by_team = {}
    for team, player in unique_players:
        if team not in players_by_team:
            players_by_team[team] = set()
        player = player.replace(' Jr.', '')
        players_by_team[team].add(player)
    return players_by_team

def fetch_all_boxscore():
    with connect_to_sql() as (cursor, conn):
        # select team and player grouped by date
        cursor.execute("SELECT date, team, player FROM player_boxscore")
        boxscore_data = cursor.fetchall()
    
    # Create a dictionary to store the players by team and date
    players_by_team_date = {}
    for date, team, player in boxscore_data:
        if date not in players_by_team_date:
            players_by_team_date[date] = {}
        if team not in players_by_team_date[date]:
            players_by_team_date[date][team] = set()
        player = player.replace(' Jr.', '')
        players_by_team_date[date][team].add(player)

    # Remove if key is n/a
    for date, team_player_dict in players_by_team_date.items():
        if 'n/a' in team_player_dict:
            del team_player_dict['n/a']

    return players_by_team_date

if __name__ == '__main__':
    players_by_team_dict = fetch_all_players()
    boxscore_by_date_dict = fetch_all_boxscore()
    injured_players_by_date = []

    for date, team_player_dict in boxscore_by_date_dict.items():
        for team, players in team_player_dict.items():
            injured_players = players_by_team_dict[team] - players
            for player in injured_players:
                injured_players_by_date.append((date, team, player))
        
    # Export the data to the MySQL database
    export_data_to_sql(injured_players_by_date, 'player_injuries')
    


