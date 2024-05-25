# CSV to JSON Parser

## Overview
This repository contains a Python script designed to read a CSV file containing user activity logs, aggregate the data by user ID, and output the results in JSON format. The script handles large datasets efficiently and accounts for common data issues like missing values, incorrect formats, or duplicate records.

## Contents
- `csv_to_json_initial.py`: Initial script using the csv module and set-based approach for deduplication.

- `csv_to_json_optimized.py`: Optimized script using the pandas library and a temporary file-based approach for handling larger datasets.

- `requirements.txt`: List of dependencies required to run the scripts.

## How to Run the Scripts
- Ensure Python 3.12.3 or higher is installed on your system.

- Install required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```
- Specify the path to your CSV file and the desired output JSON file in csv_to_json_optimized.py  

    - Open the csv_to_json_optimized.py script  
    - Locate the 'Usage' section at the end of the script.  
    - Modify the csv_path variable to point to the location of your CSV file. For example:
  
        ```bash
        csv_path = 'path/to/your/user_activities.csv'
        ```
    - Change the output_json_path variable to your desired output location for the JSON file. For example:  
  
        ```bash
        output_json_path = 'path/to/your/output/aggregated_activities.json'
        ```
    - Save the changes to the script.

- Run the script:
    ```bash
    python csv_to_json_optimized.py
    ```
    Replace csv_to_json_optimized.py with csv_to_json_initial.py to run the initial version.

## Background and Approach

The initial implementation (csv_to_json_initial.py) utilized the csv module with a set-based approach for deduplication. This approach was straightforward but not memory-efficient for very large datasets due to the overhead of storing all unique records in memory.

Given the limitations of the initial approach, I developed an optimized version (csv_to_json_optimized.py) that uses the pandas library for data handling and a temporary file to track deduplication. This method improves efficiency by reducing memory usage and handling larger datasets effectively by leveraging disk-based storage.

The choice to use a temporary file-based approach instead of a set was driven by the need to minimize memory consumption when dealing with very large datasets where a set could grow large enough to degrade performance or exceed available memory. The temporary file approach allows for handling data that surpasses memory constraints by utilizing disk storage, which is slower but more scalable.

## Explanation of the optimal code

This script processes a CSV file containing user activity logs using the pandas library to aggregate the data by user ID and output the results in JSON format. Here’s a breakdown of its main components and functionalities:

`validate_and_aggregate(chunk, temp_file_path)`
This function processes individual chunks of the CSV data. It:

- Initializes a default dictionary user_data to aggregate user information.
  
- Removes any rows in the chunk with missing essential fields (User ID, TimeStamp, Activity, Count, IP Address).

- Ensures no duplicate entries exist within the current chunk.

- Iterates over each row in the chunk to process and validate data:

    -- Constructs a unique record key for each row to check against previously seen records using a temporary file. This helps maintain uniqueness across different chunks.

    -- Validates IP addresses and timestamps to ensure data integrity.

    -- Aggregates activities, counts, timestamps, and IP addresses into user_data.

    -- Catches and logs any data inconsistencies or errors.
- Returns a dictionary containing the aggregated data for the chunk.

`merge_dicts(dict1, dict2)`
This function merges two dictionaries. This ensures that data from multiple chunks are compiled into a single dictionary efficiently.

`process_csv_pandas(file_path, chunksize)`
- Defines a user_data dictionary to store the aggregated results from all chunks.
- Manages a temporary file temp_unique_records.txt to track unique records across chunks, ensuring there are no duplicates across the entire dataset.
- Iterates through the CSV file in chunks:

    -- Validates that each chunk contains the necessary columns.

    -- Calls validate_and_aggregate to process and validate each chunk.

    -- Merges the results of each chunk with the main user_data using merge_dicts.

- Removes the temporary file after processing to clean up the storage.  

- Returns the fully aggregated data.


## Optimization and Further Improvements

While the current solution is efficient, further enhancements could include:

- `Parallel Processing`: Implementing multiprocessing to handle different chunks of the data simultaneously, thus speeding up the processing time.

- `Profiling`: Using tools like cProfile to identify bottlenecks in the code and optimize them further.

- `Testing with Larger Datasets`: Continuously testing with larger datasets to ensure scalability and making adjustments as needed.

