from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from dbfread import DBF
import os
from datetime import date
from pyspark.sql.types import *
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("DBF to CSV Conversion").getOrCreate()

# Function to read DBF and return Spark DataFrame
def read_dbf_spark(file_path):
    table = DBF(file_path, load=True, encoding='latin-1', ignore_missing_memofile=True)
    df_pandas = pd.DataFrame(iter(table))  # Load as pandas DataFrame first
    # Convert pandas DataFrame to Spark DataFrame
    df_spark = spark.createDataFrame(df_pandas)
    return df_spark

# Function to write Spark DataFrame to CSV
def write_csv_spark(df_spark, file_name):
    df_spark.write.csv(file_name, sep=';', mode='overwrite', header=True)

# Lands and file names
lands = ['F01', 'F02', 'F03', 'F04']
FILE_NAME = 'V2AD1156'

for LAND in lands:
    dbf_file_path = f'/Volumes/DATA/{LAND}/{FILE_NAME}.dbf'
    csv_file_path = f'/Volumes/MARAL/CSV/{LAND}/{FILE_NAME}.csv'

    # Check if CSV file exists
    if os.path.exists(csv_file_path):
        print(f"The file at {csv_file_path} exists.")
        modification_time = os.path.getmtime(csv_file_path)
        modification_date = date.fromtimestamp(modification_time)
        print(modification_date)
        
        today_date = date.today()
        print(today_date)
        
        if modification_date != today_date:
            # Load DBF as Spark DataFrame and write to CSV
            dbf_spark_df = read_dbf_spark(dbf_file_path)
            write_csv_spark(dbf_spark_df, csv_file_path)

        # Read CSV into Spark DataFrame
        df_spark = spark.read.csv(csv_file_path, sep=';', header=True, inferSchema=True)
    else:
        print(f"The file at {csv_file_path} does not exist.")
        # Load DBF as Spark DataFrame and write to CSV
        dbf_spark_df = read_dbf_spark(dbf_file_path)
        write_csv_spark(dbf_spark_df, csv_file_path)

        # Read newly created CSV
        df_spark = dbf_spark_df

# Stop the Spark session when done
spark.stop()
