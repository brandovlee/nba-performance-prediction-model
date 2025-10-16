"""
Automated NBA Player Data Scraper Script

This script automates the process of scraping data from various sources using web scrapers. It records the start time, runs the 
web scrapers to collect data, waits for a few seconds between each scraper execution to avoid overloading the servers, and 
then runs the projection models. Finally, it records the end time and calculates the elapsed time for the entire process.

Author: Brandon Lee
Date: April 7th, 2024

Requirements:
- Python 3.x
- Subprocess module
- Time module
- Multiprocessing module
- Web scraper scripts (located in scrapers directory)

"""
## Import libraries
import subprocess
import time
import multiprocessing

# Function to run each scraper script
def run_scraper(script):
    subprocess.run(["python", script])

if __name__ == "__main__":
    start_time = time.time() # Record Start time

    # List of scraper scripts to run
    scraper_scripts = [
        "scrapers-gamelog/scrape_boxscores.py",
        "scrapers-misc/scrape_dk_props.py",
        "scrapers-misc/scrape_games.py",
        "scrapers-injuries/scrape_injuries.py",
        "scrapers-injuries/parse_past_injuries.py",
        "scrapers-team-api/scrape_traditional_team.py",
        "scrapers-team-api/scrape_misc_team.py",
        "scrapers-team-api/scrape_playtype_team.py",
        "scrapers-team-api/scrape_zone_team.py",
        "scrapers-player-api/scrape_misc_player.py",
        "scrapers-player-api/scrape_playtype_player.py",
        "scrapers-player-api/scrape_traditional_player.py",
        "scrapers-player-api/scrape_zone_player.py"
    ]

    # Specify max number of processes
    max_processes = 1

    # Create a pool of worker processes
    pool = multiprocessing.Pool(processes=max_processes)

    # Run each scraper script asynchronously
    pool.map(run_scraper, scraper_scripts)

    pool.close()
    pool.join()

    end_time = time.time()  # Record end time
    elapsed_time = end_time - start_time  # Calculate elapsed time
    print(f"Scrapers Complete. Time taken: {elapsed_time} seconds.")
