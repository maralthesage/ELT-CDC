import os
import subprocess
import shutil
import hashlib
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
        "-lco", "STRING_QUOTING=IF_NEEDED",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    logger.info(result.stdout or "Conversion completed.")


def file_md5(path):
    """Return MD5 hash for a file to validate copy integrity."""
    hasher = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


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

    total_files = len(lands) * len(files)
    processed_files = 0
    progress_bar = tqdm(total=total_files, desc="Processing files", ncols=100, dynamic_ncols=True)

    for LAND in lands:
        for FILE_NAME in files:
            processed_files += 1
            dbf_file_path = fr'/Volumes/DATA/{LAND}/{FILE_NAME}.dbf'
            csv_file_path = fr'/Volumes/MARAL/CSV/{LAND}/{FILE_NAME}.csv'
            mirror_dbf_path = os.path.join("dbf_mirror", LAND, f"{FILE_NAME}.dbf")
            mirror_csv_path = os.path.join("csv_mirror", LAND, f"{FILE_NAME}.csv")

            print(f"\n[{datetime.now()}] Processing file {processed_files}/{total_files}: {FILE_NAME} ({LAND})")

            if not os.path.exists(dbf_file_path):
                logger.warning(f"Source missing, skipping: {dbf_file_path}")
                progress_bar.update(1)
                continue

            # Mirror the DBF locally before conversion
            os.makedirs(os.path.dirname(mirror_dbf_path), exist_ok=True)
            source_mtime = os.path.getmtime(dbf_file_path)
            source_date = datetime.fromtimestamp(source_mtime).date()
            mirror_dbf_needs_update = (
                not os.path.exists(mirror_dbf_path)
                or os.path.getsize(mirror_dbf_path) == 0
                or os.path.getmtime(mirror_dbf_path) < source_mtime
            )
            if os.path.exists(mirror_dbf_path):
                mirror_date = datetime.fromtimestamp(os.path.getmtime(mirror_dbf_path)).date()
                if mirror_date == source_date and os.path.getsize(mirror_dbf_path) > 0:
                    mirror_dbf_needs_update = False
            if mirror_dbf_needs_update:
                shutil.copy2(dbf_file_path, mirror_dbf_path)

            # Create target directory if it does not exist
            os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

            # Create mirror CSV directory if it does not exist
            os.makedirs(os.path.dirname(mirror_csv_path), exist_ok=True)

            try:
                mirror_csv_needs_update = (
                    not os.path.exists(mirror_csv_path)
                    or os.path.getsize(mirror_csv_path) == 0
                    or os.path.getmtime(mirror_csv_path) < os.path.getmtime(mirror_dbf_path)
                )
                if mirror_csv_needs_update:
                    convert_dbf_to_csv(logger, mirror_dbf_path, mirror_csv_path)

                remote_csv_needs_update = (
                    not os.path.exists(csv_file_path)
                    or os.path.getsize(csv_file_path) == 0
                    or os.path.getmtime(csv_file_path) < os.path.getmtime(mirror_csv_path)
                )
                if remote_csv_needs_update:
                    shutil.copy2(mirror_csv_path, csv_file_path)
                if os.path.exists(csv_file_path):
                    if file_md5(mirror_csv_path) != file_md5(csv_file_path):
                        shutil.copy2(mirror_csv_path, csv_file_path)
                    else:
                        logger.info(f"File {csv_file_path} is up-to-date.")
            except subprocess.CalledProcessError as e:
                logger.error(f"ogr2ogr failed for {mirror_dbf_path}: {e.stderr}")
            finally:
                progress_bar.update(1)

    progress_bar.close()

if __name__ == "__main__":
    etl_flow()
