import os
import subprocess
import logging
from datetime import datetime
from tqdm import tqdm
from logging.handlers import RotatingFileHandler

LOG_PATH = os.environ.get("DBF2CSV_LOG", "/tmp/dbf2csvogr.log")
OGR2OGR_PATH = os.environ.get("OGR2OGR_PATH", "/opt/homebrew/bin/ogr2ogr")


def setup_logger():
    """Basic rotating file + console logger."""
    logger = logging.getLogger("dbf2csvogr")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def convert_dbf_to_csv(logger, file_path, csv_path):
    """Convert a DBF file to CSV using GDAL's ogr2ogr command."""
    logger.info(f"Converting {file_path} -> {csv_path}")
    cmd = [
        OGR2OGR_PATH,
        "-f", "CSV",
        csv_path,
        file_path,
        "--config", "SHAPE_ENCODING", "CP850",
        "-lco", "SEPARATOR=SEMICOLON",
        # "-lco", "WRITE_BOM=YES",
        "-lco", "STRING_QUOTING=IF_NEEDED",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    logger.info(result.stdout or "Conversion completed.")


def etl_flow():
    """ETL flow for processing .dbf files and saving them as .csv using ogr2ogr."""
    logger = setup_logger()
    lands = ['F01', 'F02', 'F03', 'F04']
    files = [
        'V2AD1001', 'V2AD1056', 'V2AD1096', 'V2AD1156', 'V2AD1004', 'V2AD1005',
        'V2AR1001', 'V2AR1002', 'V2AR1004', 'V2AR1005', 'V2AR1007',
        'V2LA1001', 'V2LA1002', 'V2LA1003', 'V2LA1005', 'V2LA1006', 'V2LA1008',
        'V4AR1009', 'V2AD1009', 'V4LA1009', 'V2SC1010'
    ]
    # files = ['V2AD2000']

    total_files = len(lands) * len(files)
    processed_files = 0
    progress_bar = tqdm(total=total_files, desc="Processing files", ncols=100, dynamic_ncols=True)

    for LAND in lands:
        for FILE_NAME in files:
            processed_files += 1
            dbf_file_path = fr'/Volumes/DATA/{LAND}/{FILE_NAME}.dbf'
            csv_file_path = fr'/Volumes/MARAL/CSV/{LAND}/{FILE_NAME}.csv'

            print(f"\n[{datetime.now()}] Processing file {processed_files}/{total_files}: {FILE_NAME} ({LAND})")

            if not os.path.exists(dbf_file_path):
                logger.warning(f"Source missing, skipping: {dbf_file_path}")
                progress_bar.update(1)
                continue

            # Create target directory if it does not exist
            os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

            if os.path.exists(csv_file_path):
                modification_time = os.path.getmtime(csv_file_path)
                modification_date = datetime.fromtimestamp(modification_time).date()
                if modification_date == datetime.today().date():
                    logger.info(f"File {csv_file_path} is up-to-date.")
                    continue

            try:
                convert_dbf_to_csv(logger, dbf_file_path, csv_file_path)
            except subprocess.CalledProcessError as e:
                logger.error(f"ogr2ogr failed for {dbf_file_path}: {e.stderr}")
            finally:
                progress_bar.update(1)

    progress_bar.close()

if __name__ == "__main__":
    etl_flow()
