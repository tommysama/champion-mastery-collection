import sqlite3
import glob
import requests
import time
import concurrent.futures

# 1. Search for databases in the current folder and present them to the user for selection.
databases = glob.glob("*.db")
for idx, db in enumerate(databases):
    print(f"{idx + 1}. {db}")

db_choice = int(input("Select a database by number: "))
db_path = databases[db_choice - 1]

# Connect to the selected database to fetch table names
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

table_names = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
print("\nAvailable tables:")
for idx, table in enumerate(table_names):
    print(f"{idx + 1}. {table[0]}")

table_choice = int(input("Select a table by number: "))
selected_table = table_names[table_choice - 1][0]

# 2. Present a list of regions for the user to pick from.
regions = ["BR1", "EUN1", "EUW1", "JP1", "KR", "LA1", "LA2", "NA1", "OC1", "TR1", "RU"]
for idx, region in enumerate(regions):
    print(f"{idx + 1}. {region}")

region_choice = int(input("Select a region by number: "))
selected_region = regions[region_choice - 1]

# Check if the table has the required columns.
columns = cursor.execute(f"PRAGMA table_info({selected_table})").fetchall()
column_names = [column[1] for column in columns]

for i in range(1, 9):
    col_name = f"champion {i}"
    if col_name not in column_names:
        cursor.execute(f"ALTER TABLE {selected_table} ADD COLUMN '{col_name}' INTEGER")

# Define the function to fetch data
request_count = 0  # Counter for requests
start_time = time.time()  # Start time for rate limiting

def fetch_data(row):
    global request_count, start_time

    summoner_name, summoner_id, api_key, champion_3 = row
    if isinstance(champion_3, int):  # Check if champion_3 is an integer
        print(f"Skipping {summoner_name} as data already exists.")
        return None

    headers = {
        "X-Riot-Token": api_key
    }
    
    retries = 3333
    while retries > 0:
        # Rate limiting
        if request_count >= 100:
            elapsed_time = time.time() - start_time
            if elapsed_time < 120:  # If less than 2 minutes have passed
                sleep_time = 120 - elapsed_time
                print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            request_count = 0  # Reset request counter
            start_time = time.time()  # Reset start time

        response = requests.get(f"https://{selected_region}.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-summoner/{summoner_id}", headers=headers)
        request_count += 1  # Increment request counter

        if response.status_code == 200:
            champion_masteries = response.json()
            champions = []
            for i in range(1, 9):
                if i <= len(champion_masteries):
                    champion_id = champion_masteries[i-1]['championId']
                    champions.append(champion_id)
                else:
                    champions.append(None)
            print(f'Data for "{summoner_name}" added')
            return summoner_id, champions
        elif response.status_code == 429:
            print(f"Rate limit exceeded for {summoner_name}. Retrying in 5 seconds...")
            time.sleep(5)
            retries -= 1
        else:
            print(f"Failed to fetch data for {summoner_name}. Status code: {response.status_code}")
            return None

# Fetch rows to process
rows = cursor.execute(f"SELECT summoner_name, summoner_id, api_key, 'champion 3' FROM {selected_table}").fetchall()

# Use threading to speed up the process
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(fetch_data, rows))

# Update the database with the results and commit immediately after each update
for result in results:
    if result:
        summoner_id, champions = result
        for i, champion_id in enumerate(champions, 1):
            if champion_id:
                cursor.execute(f"UPDATE {selected_table} SET 'champion {i}' = ? WHERE summoner_id = ?", (champion_id, summoner_id))
        conn.commit()  # Commit changes immediately after updating each row

conn.close()