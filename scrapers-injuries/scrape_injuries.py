from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import mysql.connector
import os
from contextlib import contextmanager
from dotenv import load_dotenv
from datetime import date
import sys

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

if __name__ == '__main__':
    # Scrape the injury report from ESPN
    url = 'https://www.espn.com/nba/injuries'

    # Start the session with selenium
    options = Options()
    options.add_argument('--no-sandbox')
    options.page_load_strategy = 'eager'
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(url)

    # Scrape the injury report
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        team_tables = soup.find_all('div', class_='ResponsiveTable Table__league-injuries') 
    except Exception as e:
        print(driver.page_source)
        sys.exit(f'Error fetching injury report: {e}')


    # Define a dictionary to map abbreviations to full team names
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
    
    # Extract the injured players
    injured_players = []
    for table in team_tables:
        team_name = table.find('span', class_='injuries__teamName ml2').text
        players = table.find_all('td', class_='col-name Table__TD')
        est_return = table.find_all('td', class_='col-date Table__TD')
        injury_status = table.find_all('td', class_='col-stat Table__TD')
        injury_description = table.find_all('td', class_='col-desc Table__TD')

        for player, return_date, status, description in zip(players, est_return, injury_status, injury_description):
            # Remove 'Jr. ' from the player name
            player = player.text.replace(' Jr.', '')

            # Map team name to abbreviation
            team_name_abbreviation = team_mapping.get(team_name, team_name)
            
            # If player is guranteed or most likely to be out, add to the list
            if 'Out' in status.text:
                injured_players.append(((date.today()).strftime('%Y/%m/%d'), team_name_abbreviation, player))
    
    if len(injured_players) == 0:
        sys.exit('Error fetching injury report: No injured players found')

    export_data_to_sql(injured_players, 'injury_report')

    


