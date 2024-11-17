import requests
import csv
from datetime import datetime
import time

# Define the API endpoint and the auth token
base_url = 'https://api.electricitymap.org/v3/carbon-intensity/history?zone='
headers = {'auth-token': 'MnSINfplNTxqx'}

#zones = [
#    'DK-DK1', 'DK-DK2', 'SE', 'NO-NO1', 'NO-NO2', 'NO-NO3', 'NO-NO4', 'NO-NO5', 'FI', 'DE'
#]

zones = []
with open('european_zones.csv', 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        zones.append(row['zone'])

# Function to fetch and append data
def fetch_and_append_data(zone):
    url = f"{base_url}{zone}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        file_name = f"./carbon/{zone}.csv"

        # Read existing data from CSV
        existing_data = set()
        try:
            with open(file_name, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    existing_data.add(row['datetime'])
        except FileNotFoundError:
            pass

        # Open CSV file for appending new data
        with open(file_name, 'a', newline='') as csvfile:
            fieldnames = ['datetime', 'carbonIntensity', 'isEstimated', 'estimationMethod']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write the header only if the file is empty or new
            if not existing_data:
                writer.writeheader()

            # Write new data rows
            new_entries = 0
            for entry in data['history']:
                if entry['datetime'] not in existing_data:
                    writer.writerow({
                        'datetime': entry['datetime'],
                        'carbonIntensity': entry['carbonIntensity'],
                        'isEstimated': entry['isEstimated'],
                        'estimationMethod': entry.get('estimationMethod', '')
                    })
                    new_entries += 1

            print(f"Appended {new_entries} new entries to {file_name}")
    else:
        print(f"Failed to fetch data for {zone}. Status code: {response.status_code}, Message: {response.text}")

# Fetch and append data for all zones
#for zone in zones:
#    fetch_and_append_data(zone)

while True:
    for zone in zones:
        fetch_and_append_data(zone)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Sleeping for 1 hour... Current time: {current_time}")
    time.sleep(3600)  # Sleep for 1 hour (3600 seconds)
