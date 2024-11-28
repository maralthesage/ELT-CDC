

import dask.dataframe as dd
from dbfread import DBF, DBFNotFound
import os
from datetime import date
from prefect import task, flow
import pandas as pd

os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

@task(retries=3, retry_delay_seconds=10)
def read_dbf(file_path):
    """
    Read a .dbf file and convert it to a Dask DataFrame.
    """
    try:
        table = DBF(file_path, load=True, encoding='cp850', ignore_missing_memofile=True)
        # Convert DBF to pandas DataFrame and then to Dask
        df = dd.from_pandas(pd.DataFrame(iter(table)), npartitions=1)
        return df
    except DBFNotFound:
        print(f"DBF file {file_path} not found.")
        return None  # Return None to indicate the file was not found

@task(retries=3, retry_delay_seconds=10)
def write_csv(df, file_name):
    """
    Write a Dask DataFrame to a .csv file.
    """
    if df is not None:
        df.to_csv(file_name, sep=';', encoding='cp850', index=False, single_file=True)
    else:
        print(f"Skipping write for {file_name} because the DataFrame is None.")

@flow
def etl_flow():
    """
    ETL flow for processing .dbf files and saving them as .csv.
    """
    lands = ['F04', 'F03', 'F02', 'F01']
    files = [
        'V2AD1001', 'V2AD1056', 'V2AD1096', 'V2AD1156', 'V2AD1004', 'V2AD1005',  
        'V2AR1001', 'V2AR1002', 'V2AR1004', 'V2AR1005', 'V2AR1007', 
        'V2LA1001', 'V2LA1002', 'V2LA1003', 'V2LA1005', 'V2LA1008', 
        'V4AR1009', 'V2AD1009', 'V4LA1009'
    ]

    for LAND in lands:
        for FILE_NAME in files:
            dbf_file_path = fr'\\10.10.1.23\data\{LAND}\{FILE_NAME}.dbf'
            csv_file_path = fr'\\10.10.1.23\maral\CSV\{LAND}\{FILE_NAME}.csv'

            if os.path.exists(csv_file_path):
                print(f"The file at {csv_file_path} exists.")
                modification_time = os.path.getmtime(csv_file_path)
                modification_date = date.fromtimestamp(modification_time)
                today_date = date.today()

                if modification_date != today_date:
                    print(f"File {csv_file_path} is outdated. Reprocessing.")
                    df = read_dbf(dbf_file_path)
                    if df is not None:  # Proceed only if the DBF file was successfully read
                        write_csv(df, csv_file_path)
                else:
                    print(f"File {csv_file_path} is up-to-date.")
            else:
                print(f"The file at {csv_file_path} does not exist. Processing.")
                df = read_dbf(dbf_file_path)
                if df is not None:  # Proceed only if the DBF file was successfully read
                    write_csv(df, csv_file_path)

if __name__ == "__main__":
    etl_flow()





