# CSV to JSON Parser

## Overview
This repository contains a Python script designed to read a CSV file containing user activity logs, aggregate the data by user ID, and output the results in JSON format. The script handles large datasets efficiently and accounts for common data issues like missing values, incorrect formats, or duplicate records.

## Contents
- `csv_to_json_initial.py`: Initial script using the csv module and set-based approach for deduplication.

- `csv_to_json_optimized.py`: Optimized script using the pandas library and a temporary file-based approach for handling larger datasets.

- `requirements.txt`: List of dependencies required to run the scripts.

## How to Run the Scripts
Ensure Python 3.12.3 or higher is installed on your system.

Install required Python libraries:
```bash
pip install -r requirements.txt
```
Run the script:
```bash
python csv_to_json_optimized.py
```
Replace csv_to_json_optimized.py with csv_to_json_initial.py to run the initial version.

## Background and Approach

The initial implementation (csv_to_json_initial.py) utilized the csv module with a set-based approach for deduplication. This approach was straightforward but not memory-efficient for very large datasets due to the overhead of storing all unique records in memory.

Given the limitations of the initial approach, I developed an optimized version (csv_to_json_optimized.py) that uses the pandas library for data handling and a temporary file to track deduplication. This method improves efficiency by reducing memory usage and handling larger datasets effectively by leveraging disk-based storage.

## Why Temporary File based approach chosen over Set for deduplication?

The choice to use a temporary file-based approach instead of a set was driven by the need to minimize memory consumption when dealing with very large datasets where a set could grow large enough to degrade performance or exceed available memory. The temporary file approach allows for handling data that surpasses memory constraints by utilizing disk storage, which is slower but more scalable.

## Optimization and Further Improvements

While the current solution is efficient, further enhancements could include:

- `Parallel Processing`: Implementing multiprocessing to handle different chunks of the data simultaneously, thus speeding up the processing time.

- `Profiling`: Using tools like cProfile to identify bottlenecks in the code and optimize them further.

- `Testing with Larger Datasets`: Continuously testing with larger datasets to ensure scalability and making adjustments as needed.

