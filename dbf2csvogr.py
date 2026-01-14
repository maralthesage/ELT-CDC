import os
import argparse
import shutil
import subprocess
import logging
import tempfile
import json
from urllib.parse import quote
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from tqdm import tqdm

# =============================================================================
# CONFIGURATION
# =============================================================================

LOG_PATH = os.environ.get("DBF2CSV_LOG", "/tmp/dbf2csvogr.log")
OGR2OGR_PATH = os.environ.get("OGR2OGR_PATH", "/opt/homebrew/bin/ogr2ogr")
OGRINFO_PATH = os.environ.get("OGRINFO_PATH", "/opt/homebrew/bin/ogrinfo")
DBF_DELTA_LOG = os.environ.get("DBF_DELTA_LOG", "/tmp/dbf_delta.log")

PROJECT_ROOT = Path(__file__).resolve().parent
SOURCE_ROOT = Path(os.environ.get("DBF_MIRROR_ROOT", str(PROJECT_ROOT / "dbf_mirror")))
LOCAL_CSV_ROOT = Path(os.environ.get("LOCAL_CSV_ROOT", str(PROJECT_ROOT / "csv_mirror")))

ENCODING = "CP850"
CSV_SEPARATOR = "SEMICOLON"

MARAL_MOUNT_POINT = Path(os.environ.get("MARAL_MOUNT_POINT", "/Volumes/MARAL"))
MARAL_SMB_URL = os.environ.get("MARAL_SMB_URL", "smb://10.10.1.23/MARAL")
MARAL_SMB_USER = os.environ.get("MARAL_SMB_USER", os.environ.get("DBF_SMB_USER"))
MARAL_SMB_PASS = os.environ.get("MARAL_SMB_PASS", os.environ.get("DBF_SMB_PASS"))
REQUIRE_MARAL_MOUNT = os.environ.get("MARAL_REQUIRE_MOUNT", "1") not in ("0", "false", "False")
TARGET_ROOT = Path(os.environ.get("TARGET_CSV_ROOT", str(MARAL_MOUNT_POINT / "CSV")))

DBF_MOUNT_POINT = Path(os.environ.get("DBF_MOUNT_POINT", "/Volumes/DATA"))
DBF_SMB_URL = os.environ.get("DBF_SMB_URL", "smb://10.10.1.23/Data")
DBF_SMB_USER = os.environ.get("DBF_SMB_USER")
DBF_SMB_PASS = os.environ.get("DBF_SMB_PASS")
DBF_SMB_DOMAIN = os.environ.get("DBF_SMB_DOMAIN")
REQUIRE_DBF_MOUNT = os.environ.get("DBF_REQUIRE_MOUNT", "1") not in ("0", "false", "False")
DBF_SOURCE_MODE = os.environ.get("DBF_SOURCE_MODE", "mounted")  # mounted or ssh
DBF_RSYNC_HOST = os.environ.get("DBF_SRC_HOST", "10.10.1.23")
DBF_RSYNC_PATH = os.environ.get("DBF_SRC_PATH", "/Data")

LANDS = ["F01", "F02", "F03", "F04"]
FILES = [
    "V2AD1001", "V2AD1056", "V2AD1096", "V2AD1156", "V2AD1004", "V2AD1005",
    "V2AR1001", "V2AR1002", "V2AR1004", "V2AR1005", "V2AR1007",
    "V2LA1001", "V2LA1002", "V2LA1003", "V2LA1005", "V2LA1006", "V2LA1008",
    "V4AR1009", "V2AD1009", "V4LA1009", "V2SC1010"
]

# =============================================================================
# LOGGING
# =============================================================================

def setup_logger():
    logger = logging.getLogger("dbf2csvogr")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger

# =============================================================================
# UTILITIES
# =============================================================================

def mask_smb_url(url: str) -> str:
    if "smb://" not in url:
        return url
    try:
        scheme, rest = url.split("smb://", 1)
    except ValueError:
        return url
    if "@" not in rest:
        return "smb://" + rest
    creds, host_path = rest.split("@", 1)
    if ":" in creds:
        user, _ = creds.split(":", 1)
        masked = f"{user}:****"
    else:
        masked = "****"
    return f"smb://{masked}@{host_path}"


def build_smb_url(base_url: str, user: str | None, password: str | None, domain: str | None = None) -> str:
    if not user or not password:
        return base_url
    user_part = f"{domain};{user}" if domain else user
    user_enc = quote(user_part, safe="")
    pass_enc = quote(password, safe="")
    return base_url.replace("smb://", f"smb://{user_enc}:{pass_enc}@", 1)


def ensure_maral_mount(logger):
    if not REQUIRE_MARAL_MOUNT:
        logger.info("MARAL mount check disabled via MARAL_REQUIRE_MOUNT.")
        return

    if MARAL_MOUNT_POINT.exists() and os.path.ismount(MARAL_MOUNT_POINT):
        logger.info("MARAL drive mounted: %s", MARAL_MOUNT_POINT)
        return

    try:
        MARAL_MOUNT_POINT.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        raise RuntimeError(
            f"Cannot create mount point {MARAL_MOUNT_POINT}. "
            "Set MARAL_MOUNT_POINT to a user-writable path (e.g. ~/maral_mount) "
            "or create /Volumes/MARAL with sudo."
        ) from exc

    smb_url = build_smb_url(MARAL_SMB_URL, MARAL_SMB_USER, MARAL_SMB_PASS)

    cmd = ["mount_smbfs", smb_url, str(MARAL_MOUNT_POINT)]
    logger.info("Mounting MARAL drive: %s", MARAL_MOUNT_POINT)
    logger.info("MARAL SMB URL: %s", mask_smb_url(smb_url))

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if "File exists" in stderr:
            logger.info("MARAL drive already mounted: %s", MARAL_MOUNT_POINT)
            return
        raise RuntimeError(stderr or "mount_smbfs failed for MARAL")


def ensure_dbf_mount(logger):
    if DBF_SOURCE_MODE != "mounted":
        logger.info("DBF mount check skipped (DBF_SOURCE_MODE=%s).", DBF_SOURCE_MODE)
        return

    if not REQUIRE_DBF_MOUNT:
        logger.info("DBF mount check disabled via DBF_REQUIRE_MOUNT.")
        return

    if DBF_MOUNT_POINT.exists() and os.path.ismount(DBF_MOUNT_POINT):
        logger.info("DBF drive mounted: %s", DBF_MOUNT_POINT)
        return

    DBF_MOUNT_POINT.mkdir(parents=True, exist_ok=True)

    smb_url = build_smb_url(DBF_SMB_URL, DBF_SMB_USER, DBF_SMB_PASS, DBF_SMB_DOMAIN)

    cmd = ["mount_smbfs", smb_url, str(DBF_MOUNT_POINT)]
    logger.info("Mounting DBF drive: %s", DBF_MOUNT_POINT)
    logger.info("DBF SMB URL: %s", mask_smb_url(smb_url))

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if "File exists" in stderr:
            logger.info("DBF drive already mounted: %s", DBF_MOUNT_POINT)
            return
        raise RuntimeError(stderr or "mount_smbfs failed for DBF")


def build_rsync_cmd():
    if DBF_SOURCE_MODE == "mounted":
        rsync_source = f"{str(DBF_MOUNT_POINT).rstrip('/')}/"
    else:
        rsync_source = f"{DBF_RSYNC_HOST}:{DBF_RSYNC_PATH}/"

    return [
        "rsync",
        "-avz",
        "--delete",
        "--prune-empty-dirs",
        "--include=*/",
        "--include=*.dbf",
        "--include=*.DBF",
        "--include=*.dbt",
        "--include=*.DBT",
        "--include=*.cpg",
        "--include=*.CPG",
        "--include=*.prj",
        "--include=*.PRJ",
        "--exclude=*",
        rsync_source,
        str(SOURCE_ROOT),
    ]


def test_mounts():
    logger = setup_logger()
    logger.info("----- DBF2CSV MOUNT TEST STARTED -----")
    ensure_dbf_mount(logger)
    ensure_maral_mount(logger)
    logger.info("----- DBF2CSV MOUNT TEST FINISHED -----")


def run_dbf_mirror_sync(logger):
    SOURCE_ROOT.mkdir(parents=True, exist_ok=True)
    cmd = build_rsync_cmd()
    logger.info("Running DBF mirror sync: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "rsync failed")

    if result.stdout:
        logger.info(result.stdout.strip())


def get_dbf_row_count(dbf_path: Path) -> int:
    cmd = [
        OGRINFO_PATH,
        "-al",
        "-geom=NO",
        str(dbf_path),
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="cp850",
        errors="replace",
        check=True,
    )

    for line in result.stdout.splitlines():
        if line.strip().startswith("Feature Count:"):
            return int(line.split(":")[1].strip())

    raise RuntimeError(f"Could not determine row count for {dbf_path}")


def get_csv_row_count(csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f) - 1


def convert_dbf_to_csv(logger, dbf_path: Path) -> Path:
    tmp_dir_path = Path(tempfile.mkdtemp(prefix="dbf2csv_"))
    shutil.rmtree(tmp_dir_path)

    cmd = [
        OGR2OGR_PATH,
        "-f", "CSV",
        str(tmp_dir_path),
        str(dbf_path),
        "--config", "SHAPE_ENCODING", ENCODING,
        "-lco", "ENCODING=UTF-8",
        "-lco", f"SEPARATOR={CSV_SEPARATOR}",
        "-lco", "STRING_QUOTING=IF_NEEDED",
    ]

    logger.info(f"Converting {dbf_path} -> {tmp_dir_path}")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ogr2ogr failed")

    tmp_csv = tmp_dir_path / f"{dbf_path.stem}.csv"
    if not tmp_csv.exists():
        raise RuntimeError(f"ogr2ogr did not produce {tmp_csv}")

    final_tmp = Path(tempfile.mkstemp(prefix=f"{dbf_path.stem}_", suffix=".csv")[1])
    shutil.move(str(tmp_csv), str(final_tmp))
    shutil.rmtree(tmp_dir_path, ignore_errors=True)

    return final_tmp

# =============================================================================
# ETL FLOW
# =============================================================================

def etl_flow():
    logger = setup_logger()
    ensure_dbf_mount(logger)
    run_dbf_mirror_sync(logger)
    ensure_maral_mount(logger)

    total_files = len(LANDS) * len(FILES)
    processed_files = 0
    progress_bar = tqdm(total=total_files, desc="Processing files", ncols=100, dynamic_ncols=True)

    for land in LANDS:
        for file_name in FILES:
            processed_files += 1
            dbf_file_path = SOURCE_ROOT / land / f"{file_name}.dbf"
            local_csv_path = LOCAL_CSV_ROOT / land / f"{file_name}.csv"
            target_csv_path = TARGET_ROOT / land / f"{file_name}.csv"

            print(f"\n[{datetime.now()}] Processing file {processed_files}/{total_files}: {file_name} ({land})")

            if not dbf_file_path.exists():
                logger.warning(f"Source missing, skipping: {dbf_file_path}")
                progress_bar.update(1)
                continue

            local_csv_path.parent.mkdir(parents=True, exist_ok=True)
            target_csv_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                dbf_rows = get_dbf_row_count(dbf_file_path)
                tmp_csv = convert_dbf_to_csv(logger, dbf_file_path)

                csv_rows = get_csv_row_count(tmp_csv)
                if dbf_rows != csv_rows:
                    logger.warning(
                        f"Row mismatch for {file_name}: DBF={dbf_rows}, CSV={csv_rows}"
                    )

                if local_csv_path.exists() and local_csv_path.is_dir():
                    shutil.rmtree(local_csv_path)
                os.replace(tmp_csv, local_csv_path)

                if target_csv_path.exists() and target_csv_path.is_dir():
                    shutil.rmtree(target_csv_path)
                tmp_target = target_csv_path.with_name(f".{target_csv_path.name}.tmp")
                shutil.copy2(local_csv_path, tmp_target)
                os.replace(tmp_target, target_csv_path)

                logger.info(f"SUCCESS {land}/{file_name} rows={dbf_rows}")
            except Exception as e:
                logger.error(f"FAILED {land}/{file_name}: {e}")
            finally:
                progress_bar.update(1)

    progress_bar.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DBF to CSV ETL")
    parser.add_argument(
        "--test-mounts",
        action="store_true",
        help="Only test SMB mounts and exit.",
    )
    args = parser.parse_args()

    if args.test_mounts:
        test_mounts()
    else:
        etl_flow()
