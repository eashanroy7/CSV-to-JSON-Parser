import csv
import json
from collections import defaultdict
import re
from datetime import datetime

# Function to read CSV and process data - returns user-wise aggregated data in the form of a dictionary
def process_csv(file_path):

    # Dictionary to store user-wise aggregated data 
    user_data = defaultdict(lambda: {

        # if a non-existent key is accessed, it will be assigned with this default dictionary as the value
        'activities': [],
        'total_count': 0,
        'timestamps': [],
        'ip_addresses': []
    })

    # Set to track seen records for duplicate detection
    seen_records = set()

    # Reading the CSV file
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:

        reader = csv.DictReader(file) # turns every row in csv into dictionary where key is the column name and value is the respective row value

        '''
        Example entry in reader:
        {'User ID': 'user_0075', 'TimeStamp': '2023-05-05 15:20:39', 'Activity': 'login', 'Count': '4', 'IP Address': '109.204.222.253'}
        '''

        for row in reader:
            try:
                # Check for missing or empty essential fields
                if not all(row[key] for key in ['User ID', 'TimeStamp', 'Activity', 'Count', 'IP Address']):
                    raise ValueError("Missing or empty essential data fields.")
                
                user_id = row['User ID']
                timestamp = row['TimeStamp']
                activity = row['Activity']
                count = int(row['Count']) # This will raise ValueError if 'Count' is not an integer
                ip_address = row['IP Address']

                if not re.match(r'^\d+\.\d+\.\d+\.\d+$', ip_address):
                        raise ValueError("Invalid IP address format.")

                # Validate timestamp format
                datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")  # This will raise ValueError if timestamp is in incorrect format

                # Check for duplicate records based on a unique combination
                record_key = (user_id, timestamp, activity, count, ip_address)
                if record_key in seen_records:
                    raise ValueError("Duplicate record detected.")
                seen_records.add(record_key)
                
                ''' 
                Aggregate data
                Appends to list contained within dictionary values
                '''
                user_data[user_id]['activities'].append(activity) 
                user_data[user_id]['total_count'] += count
                user_data[user_id]['timestamps'].append(timestamp)
                user_data[user_id]['ip_addresses'].append(ip_address)

                '''
                Example entry in user_data:
                'user_0075': {
                    'activities': ['login'],
                    'total_count': 4,
                    'timestamps': ['2023-05-05 15:20:39'],
                    'ip_addresses': ['109.204.222.253']
                }
                '''
            except ValueError as e:
                print(f"Error processing row {row}: {e}")

    return user_data

# Function to write data to JSON file
def write_json(data, output_file): # takes 'dictionary with aggregated data', and 'output file path' as arguments 
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4) # serializes dictionary to json format

# Usage
# Update the paths below to match the location of your CSV file and where you want the JSON output to be saved
csv_path = 'user_activities.csv' # Path to the input CSV file. Change this to the path of your CSV file.
output_json_path = 'aggregated_activities.json' # Path to the output JSON file. Change this to your desired output path.
aggregated_data = process_csv(csv_path)
write_json(aggregated_data, output_json_path)
print("JSON file has been created successfully with aggregated data.")