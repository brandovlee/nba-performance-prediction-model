## Import libraries
import os
import requests
import mysql.connector
from contextlib import contextmanager
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

def create_table(cursor, table_name):
    """Drops table if exists then creates a new table in the MySQL database """
    drop_table_query = f'''
    DROP TABLE IF EXISTS {table_name}
    '''
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Player VARCHAR(255),
        Line FLOAT
    )
    '''
    cursor.execute(drop_table_query)
    cursor.execute(create_table_query)

def insert_data(cursor, data, table_name):
    """Inserts data into the MySQL database """
    insert_query = f'''
    INSERT INTO `{table_name}` (`Player`, `Line`) 
    VALUES (%s, %s)
    '''
    cursor.executemany(insert_query, data)

def export_data_to_sql(data, table_name):
    """Exports the data to the MySQL database """
    with connect_to_sql() as (cursor, conn):
        # Create table
        create_table(cursor, table_name)

        # Check that player name matches NBA.com
        check_names(cursor, data)

        # Insert data
        data_to_insert = [(player['player'], player['line']) for player in data]
        insert_data(cursor, data_to_insert, table_name)
        conn.commit()

def scrape_data(url):
        headers = {
            'accept': '*/*',
            'origin': 'https://sportsbook.draftkings.com',
            'referer': 'https://sportsbook.draftkings.com/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }
        response = requests.get(url, headers=headers)
        return response.json()

def parse_data(data):    
    # Store the output data
    output_data = []

    # Get the prop name
    market = data['markets'][0]
    market_type_name = market['marketType']['name']
    prop_name = market_type_name.replace("O/U", "").strip()
    player_visited = set()

    for selection in data['selections']:
        # Get the player name and stat value
        player_name = selection['participants'][0]['name']
        stat_value = selection['points']

        # Handle special cases for player names
        if player_name == "Cameron Thomas":
            player_name = "Cam Thomas"
        elif player_name == "Nicolas Claxton":
            player_name = "Nic Claxton"
        elif player_name == "Robert Williams":
            player_name = "Robert Williams III"
        elif player_name == "Alexandre Sarr":
            player_name = "Alex Sarr"
        elif player_name == "Carlton Carrington":
            player_name = "Bub Carrington"
        elif player_name == "Jaylin Williams (OKC)":
            player_name = "Jaylin Williams"
        elif player_name == "Jimmy Butler":
            player_name = "Jimmy Butler III"

        # Remove Jr. from player name
        player_name = player_name.replace(' Jr.', '') # Remove 'Jr.' from player name
        
        # Append the base data to the output_data if the player has not been visited
        if player_name not in player_visited:
            output_data.append({
                'player': player_name,
                'prop': prop_name,
                'line': stat_value,
            })
            player_visited.add(player_name)
    return output_data
    
def check_names(cursor, data):
    try:
        # Query to check if player names are in the MySQL database
        query = "SELECT Player FROM player_traditional"
        cursor.execute(query)
        result = cursor.fetchall()

        # Store the player names in a set
        player_names = set()
        for player_data in result:
            player = player_data[0]
            player_names.add(player)
        
        # Iterate through the player names in the data
        for player in data:
            if player['player'] not in player_names:
                print(f"WARNING: {player['player']} not found on NBA.com")

    except Exception as e:
        print("An error occurred:", e)
        # Exit the script upon encountering an error
        exit()

def main():
    url = "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusor/v1/leagues/42648/categories/1215/subcategories/12488"
    html_contents = scrape_data(url)
    data = parse_data(html_contents)

    # Export Data
    export_data_to_sql(data, "dk_props")
    print("dk_props data saved.")

# Call the main function
if __name__ == "__main__":
    main()