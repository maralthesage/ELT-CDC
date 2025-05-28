import json
import os
from datetime import datetime
from dbfread import DBF
import pandas as pd
import time


def folder_maker(filename, land):
    dbf_file_path = f"/Volumes/DATA/{land}/{filename}.dbf"
    csv_file_path = f"/Volumes/MARAL/CSV/{land}/{filename}.csv"
    metadata_file_path = f"/Volumes/MARAL/CSV/{land}/META/{filename}.json"
    return dbf_file_path, csv_file_path, metadata_file_path


def get_max_timestamp_from_dbf(dbf_file_path, timestamp_field):
    """Get the maximum timestamp from the DBF file without loading all records."""
    max_timestamp = datetime(1980, 1, 1)  # Default to a very old date

    dbf_table = DBF(dbf_file_path, encoding="latin-1", ignore_missing_memofile=True)

    for record in dbf_table:
        if timestamp_field in record:
            try:
                timestamp = pd.to_datetime(record[timestamp_field], errors="coerce")
                if pd.notna(timestamp) and timestamp > max_timestamp:
                    max_timestamp = timestamp
            except:
                continue

    return max_timestamp


def read_dbf(dbf_file_path):
    """Read DBF file and return records as a list of dictionaries."""
    dbf_table = DBF(
        dbf_file_path, load=True, encoding="latin-1", ignore_missing_memofile=True
    )
    return [dict(record) for record in dbf_table]


def get_last_timestamp(metadata_file_path):
    """Check metadata file for the last processed timestamp."""
    if os.path.exists(metadata_file_path):
        with open(metadata_file_path, "r") as f:
            metadata = json.load(f)
            return datetime.strptime(
                metadata["last_processed_timestamp"], "%Y-%m-%d %H:%M:%S"
            )
    return datetime(1980, 1, 1)  # Default to a very old date


def process_and_update_csv(
    dbf_file_path, csv_file_path, metadata_file_path, timestamp_field
):
    """Process DBF file and update CSV and metadata file."""
    # Check the last processed timestamp
    last_processed_timestamp = get_last_timestamp(metadata_file_path)

    # Get the maximum timestamp from the DBF file
    max_timestamp = get_max_timestamp_from_dbf(dbf_file_path, timestamp_field)

    if max_timestamp <= last_processed_timestamp:
        print("No new records to process.")
        return

    # Read the full DBF file if there are new records
    records = read_dbf(dbf_file_path)

    # Convert records to DataFrame
    df = pd.DataFrame(records)

    # Convert the timestamp_field to datetime if it's not already
    if pd.api.types.is_string_dtype(df[timestamp_field]):
        df[timestamp_field] = pd.to_datetime(df[timestamp_field], errors="coerce")
    elif not pd.api.types.is_datetime64_any_dtype(df[timestamp_field]):
        df[timestamp_field] = pd.to_datetime(df[timestamp_field], errors="coerce")

    # Ensure that all values in the timestamp field are datetime objects
    df[timestamp_field] = df[timestamp_field].apply(
        lambda x: x if isinstance(x, pd.Timestamp) else pd.NaT
    )

    # Filter new records
    new_records_df = df[df[timestamp_field] > pd.Timestamp(last_processed_timestamp)]
    ### -------------- added to remove double lines added through other loadings of the file ------------- ###
    new_records_df = new_records_df.drop_duplicates()
    # Write new records to CSV
    if not new_records_df.empty:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        # Write to CSV with a semicolon delimiter and latin-1 encoding
        new_records_df.to_csv(
            csv_file_path, mode="a", sep=";", encoding="latin-1", index=False
        )

        # Update or create metadata file
        latest_timestamp = new_records_df[timestamp_field].max()
        metadata = {
            "last_processed_timestamp": latest_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(metadata_file_path, "w") as f:
            json.dump(metadata, f)


def incremental_load_process(filename, timestamp_field, land):
    """Chain all steps to perform incremental load from DBF to CSV."""

    dbf_file_path, csv_file_path, metadata_file_path = folder_maker(filename, land)
    # Read DBF file
    print(f"Reading DBF file from {dbf_file_path}...")
    records = read_dbf(dbf_file_path)
    print(f"Read {len(records)} records from DBF file.")

    # Process DBF file and update CSV
    print(f"Processing DBF file and updating CSV files...")
    process_and_update_csv(
        dbf_file_path, csv_file_path, metadata_file_path, timestamp_field
    )
    print(f"CSV files and metadata updated.")


files = [
    "V2AD1001",
    "V2AD1004",
    "V2AD1005",
    "V2AD1009",
    "V2AR1001",
    "V2AR1002",
    "V2AR1004",
    "V2AR1005",
    "V2AR1006",
    "V2AR1007",
    "V2LA1001",
    "V2LA1003",
    "V2LA1005",
    "V2LA1008",
    "V4AR1009",
    "V4LA1009",
    "V2AD1056",
    "V2AD1096",
]

land = "F01"

for file in files:
    start_time = time.time()
    timestamp_field = "SYS_BEWEG"
    if file == "V2AD1056":
        timestamp_field = "AUF_ANLAGE"
    elif file == "V2AD1096":
        timestamp_field = "BEST_AM"

    incremental_load_process(file, timestamp_field, land)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")
