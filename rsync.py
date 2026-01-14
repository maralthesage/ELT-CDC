import os
import subprocess
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

LOG_PATH = os.environ.get("DBF_RSYNC_LOG", "/tmp/dbf_rsync.log")

RSYNC_SRC_HOST = "10.10.1.23"
RSYNC_SRC_PATH = "/Data"
RSYNC_DEST = "/maralsheikhzadeh/rsync"

LANDS = ['F01', 'F02', 'F03', 'F04']
FILES = [
    'V2AD1001', 'V2AD1056', 'V2AD1096', 'V2AD1156', 'V2AD1004', 'V2AD1005',
    'V2AR1001', 'V2AR1002', 'V2AR1004', 'V2AR1005', 'V2AR1007',
    'V2LA1001', 'V2LA1002', 'V2LA1003', 'V2LA1005', 'V2LA1006', 'V2LA1008',
    'V4AR1009', 'V2AD1009', 'V4LA1009', 'V2SC1010'
]


def setup_logger():
    logger = logging.getLogger("dbf_rsync")
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


def remote_file_exists(land, filename):
    """Checks if a file exists on the remote server using ssh."""
    remote_file = f"{RSYNC_SRC_PATH}/{land}/{filename}.dbf"
    cmd = ["ssh", RSYNC_SRC_HOST, f"test -f {remote_file} && echo OK || echo NO"]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout.strip() == "OK"


def build_rsync_includes(logger):
    includes = []
    for land in LANDS:
        land_included = False

        for fname in FILES:
            if remote_file_exists(land, fname):
                includes.append(f"--include={land}/{fname}.dbf")
                land_included = True
            else:
                logger.warning(f"Missing: {land}/{fname}.dbf (skipped)")

        if land_included:
            includes.append(f"--include={land}/")  # include folder

    return includes


def rsync_dbf_files(logger):
    logger.info("Scanning remote folders for existing DBF files...")

    include_rules = build_rsync_includes(logger)

    rsync_cmd = [
        "rsync",
        "-avz",
        "--progress",
        "--delete",
    ]
    rsync_cmd.extend(include_rules)
    rsync_cmd.append("--exclude=*")

    rsync_cmd.append(f"{RSYNC_SRC_HOST}:{RSYNC_SRC_PATH}/")
    rsync_cmd.append(RSYNC_DEST)

    logger.info("Running rsync command:")
    logger.info(" ".join(rsync_cmd))

    try:
        result = subprocess.run(rsync_cmd, capture_output=True, text=True, check=True)
        logger.info(result.stdout or "rsync completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"rsync failed: {e.stderr}")
        raise


def main():
    logger = setup_logger()
    start = datetime.now()
    logger.info("----- DBF RSYNC STARTED -----")

    try:
        rsync_dbf_files(logger)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    duration = datetime.now() - start
    logger.info(f"----- DBF RSYNC FINISHED ({duration}) -----")


if __name__ == "__main__":
    main()
