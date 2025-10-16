## Import libraries
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from datetime import date
from contextlib import contextmanager
import mysql.connector
from dotenv import load_dotenv

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

def scrape_data(url):
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('headless=new') # headless mode
    driver = webdriver.Chrome(options=options)

    all_html_content = []

    driver.get(url)

    # Wait until at least one matchup card is present
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'GameCardMatchup_wrapper__uUdW8')))

    # Find all matchup cards
    matchup_cards = driver.find_elements(By.CLASS_NAME, 'GameCardMatchup_wrapper__uUdW8')

    # Extract html content of each individual matchup
    for matchup_card in matchup_cards:
        all_html_content.append(matchup_card.get_attribute('outerHTML'))

    driver.quit()

    return all_html_content

def parse_table(html_contents):
    awayTeam = []
    homeTeam = []
    i = 0 # Counter to alternate teams

    for html_content in html_contents:
        soup = BeautifulSoup(html_content, 'html.parser')


        # Define a dictionary to map abbreviations to full team names
        team_mapping = {
            'Jazz': 'UTA', 'Bulls': 'CHI', 'Suns': 'PHX',
            'Warriors': 'GSW', 'Hornets': 'CHA', 'Heat': 'MIA',
            'Grizzlies': 'MEM', 'Mavericks': 'DAL', 'Pelicans': 'NOP',
            'Thunder': 'OKC', 'Lakers': 'LAL', 'Raptors': 'TOR',
            'Hawks': 'ATL', 'Bucks': 'MIL', 'Wizards': 'WAS',
            'Kings': 'SAC', 'Pistons': 'DET', '76ers': 'PHI',
            'Knicks': 'NYK', 'Clippers': 'LAC', 'Cavaliers': 'CLE',
            'Rockets': 'HOU', 'Celtics': 'BOS', 'Nets': 'BKN',
            'Nuggets': 'DEN', 'Magic': 'ORL', 'Trail Blazers': 'POR',
            'Pacers': 'IND', 'Spurs': 'SAS', 'Timberwolves': 'MIN'
        }

        team_names = soup.find_all('span', class_='MatchupCardTeamName_teamName__9YaBA')
        
        # Extract the text from each element and add it to the respective team list
        for team_name in team_names:
            # Convert team name to abbreviation using the mapping dictionary
            team_name_text = team_name.text.strip()  # Get the text content of the element
            team_name_abbreviation = team_mapping.get(team_name_text, team_name_text)
            if  i % 2 == 0:
                awayTeam.append(team_name_abbreviation)
            else:
                homeTeam.append(team_name_abbreviation)
            i += 1
    
    # Combine the team lists into a dictionary
    teams = {'Away_Team': awayTeam, 'Home_Team': homeTeam}

    return teams
        
def create_table(cursor, table_name):
    """Drops table if it exists then creates new one in MySQL database"""
    # Define the query to drop table
    drop_table_query = f'''
    DROP TABLE IF EXISTS {table_name}
    '''

    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        `Away_Team` VARCHAR(255),
        `Home_Team` VARCHAR(255)
    )
    '''
    cursor.execute(drop_table_query)
    cursor.execute(create_table_query)

def insert_data(cursor, data, table_name):
    """Inserts data into the MySQL database """
    insert_query = f'''
    INSERT INTO `{table_name}` (`Away_Team`, `Home_Team`) 
    VALUES (%s, %s)
    '''
    cursor.executemany(insert_query, data)

def export_data_to_sql(data, table_name):
    """Exports the data to the MySQL database """
    with connect_to_sql() as (cursor, conn):
        # Create table
        create_table(cursor, table_name)

        # Insert data
        data_to_insert = [(data['Away_Team'][i], data['Home_Team'][i]) for i in range(len(data['Away_Team']))]

        insert_data(cursor, data_to_insert, table_name)
        conn.commit()

def main():
    # Get today's date
    today = date.today()
    year, month, day = today.year, today.month, today.day
    # Add leading zero to month and day if less than 10
    if day < 10:
        day = f"0{day}"
    if month < 10:
        month = f"0{month}"
    # Convert target_date to date object
    # target_date = "11/29/2024"
    # target_date = datetime.strptime(target_date, '%m/%d/%Y').date()
    # year, month, day = target_date.year, target_date.month, target_date.day

    # Scrape the data
    url = f"https://www.nba.com/games?date={year}-{month}-{day}"
    html_contents = scrape_data(url)
    data = parse_table(html_contents)

    # Export Data
    export_data_to_sql(data, 'nba_matchups')
    print("nba_matchups saved.")

# Call the main function
if __name__ == "__main__":
    main()
