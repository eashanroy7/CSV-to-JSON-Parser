import pandas as pd
import numpy
import json
import re
from datetime import datetime
from collections import defaultdict
import os

def validate_and_aggregate(chunk, temp_file_path):
    # Empty dictionary to store user-wise aggregated data 
    user_data = defaultdict(lambda: {
        # if a non-existent key is accessed, it will be assigned with this default dictionary as the value
        'activities': [],
        'total_count': 0,
        'timestamps': [],
        'ip_addresses': []
    })

    # Drop rows with any missing essential fields to ensure data integrity
    if chunk[['User ID', 'TimeStamp', 'Activity', 'Count', 'IP Address']].isnull().any().any():
        print("Skipping rows with missing or empty essential fields in this chunk.")
        chunk.dropna(subset=['User ID', 'TimeStamp', 'Activity', 'Count', 'IP Address'], inplace=True)

    # Remove duplicate entries within the current chunk based on unique identifiers
    chunk = chunk.drop_duplicates(subset=['User ID', 'TimeStamp', 'Activity', 'Count', 'IP Address'])

    # Looping through each row in the current chunk
    for _, row in chunk.iterrows():
        try:
            user_id = str(row['User ID'])
            timestamp = str(row['TimeStamp'])
            activity = str(row['Activity'])
            count = int(row['Count'])
            ip_address = str(row['IP Address'])

            # Create a unique record key for checking duplicates across chunks
            record_key = f"{user_id},{timestamp},{activity},{count},{ip_address}\n"

            # Check if this record has been seen in previous chunks by checking a temporary file
            with open(temp_file_path, 'r') as temp_file:
                if record_key in temp_file.read():
                    continue

            # Write the new unique record to the temp file
            with open(temp_file_path, 'a') as temp_file:
                temp_file.write(record_key)

            # Validate IPv4 address format to ensure data quality
            if not re.match(r'^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$', ip_address):
                raise ValueError("Invalid IPv4 address format.")

            # Validate the format of the timestamp
            datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")  # This will raise ValueError if timestamp is in incorrect format

            # Aggregate validated data into user_data
            # Appends to list contained within the dictionary values of user_data
            user_data[user_id]['activities'].append(activity)
            user_data[user_id]['total_count'] += count
            user_data[user_id]['timestamps'].append(timestamp)
            user_data[user_id]['ip_addresses'].append(ip_address)

        except ValueError as e:
            print(f"Error processing row {row.to_dict()}: {e}")

    # Returns aggregated and validated data in the form of a dictionary for the current chunk
    return user_data

def merge_dicts(dict1, dict2):
    # Merge data from two dictionaries, combining lists and summing counts

    for key, value in dict2.items():
        # For each key found in dict2, the corresponding lists in dict1 are extended with the lists from dict2.
        dict1[key]['activities'].extend(value['activities'])
        dict1[key]['total_count'] += value['total_count']
        dict1[key]['timestamps'].extend(value['timestamps'])
        dict1[key]['ip_addresses'].extend(value['ip_addresses'])

        '''
        This is how a sample entry from dict1 or dict2 looks like:

        'user_0075': {
            'activities': ['login'],
            'total_count': 4,
            'timestamps': ['2023-05-05 15:20:39'],
            'ip_addresses': ['109.204.222.253']
        }
        '''
    return dict1

def process_csv_pandas(file_path, chunksize=5000):
    user_data = defaultdict(lambda: {
        'activities': [],
        'total_count': 0,
        'timestamps': [],
        'ip_addresses': []
    })

    # Temporary file to store unique records for cross-chunk duplicate checking
    temp_file_path = 'temp_unique_records.txt'

    # Create or clear the temporary file
    open(temp_file_path, 'w').close()

    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        # chunk is a DataFrame of size = chunksize
        
        # Ensure all necessary columns are present
        expected_columns = {'User ID', 'TimeStamp', 'Activity', 'Count', 'IP Address'}
        if not expected_columns.issubset(chunk.columns):
            missing_cols = expected_columns - set(chunk.columns)
            raise KeyError(f"Missing expected columns: {', '.join(missing_cols)}")

        # Validate and aggregate data from the chunk
        chunk_data = validate_and_aggregate(chunk, temp_file_path)
        user_data = merge_dicts(user_data, chunk_data)

    # Clean up the temporary file after processing
    os.remove(temp_file_path)

    # Returns aggregated and validated data in the form of a dictionary for all the chunks
    return user_data

def write_json(data, output_file):
    # Serialize dictionary to JSON and write it to a file
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

# Usage
csv_path = 'user_activities.csv'
output_json_path = 'aggregated_activities.json'
try:
    aggregated_data = process_csv_pandas(csv_path)
    write_json(aggregated_data, output_json_path)
    print("JSON file has been created successfully with aggregated data.")
except Exception as e:
    print(e)
