import argparse
import json
import logging
import os
import subprocess
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_PATH = os.environ.get("DBF_DELTA_LOG", "/tmp/dbf_delta.log")

RSYNC_SRC_HOST = os.environ.get("DBF_SRC_HOST", "10.10.1.23")
RSYNC_SRC_PATH = os.environ.get("DBF_SRC_PATH", "/Data")

MIRROR_ROOT = Path(
    os.environ.get(
        "DBF_MIRROR_ROOT",
        str(Path(__file__).resolve().parent / "dbf_mirror"),
    )
)
STATE_FILE = MIRROR_ROOT / ".delta_state.json"

MOUNT_POINT = Path(os.environ.get("DBF_MOUNT_POINT", "/Volumes/DATA"))
SMB_URL = os.environ.get("DBF_SMB_URL", "smb://10.10.1.23/Data")
SMB_USER = os.environ.get("DBF_SMB_USER")
SMB_PASS = os.environ.get("DBF_SMB_PASS")
REQUIRE_MOUNT = os.environ.get("DBF_REQUIRE_MOUNT", "1") not in ("0", "false", "False")
SOURCE_MODE = os.environ.get("DBF_SOURCE_MODE", "mounted")  # mounted or ssh


def setup_logger():
    logger = logging.getLogger("dbf_delta")
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


def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def build_rsync_cmd():
    if SOURCE_MODE == "mounted":
        rsync_source = f"{str(MOUNT_POINT).rstrip('/')}/"
    else:
        rsync_source = f"{RSYNC_SRC_HOST}:{RSYNC_SRC_PATH}/"

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
        str(MIRROR_ROOT),
    ]


def run_rsync(logger):
    MIRROR_ROOT.mkdir(parents=True, exist_ok=True)
    cmd = build_rsync_cmd()
    logger.info("Running: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "rsync failed")

    if result.stdout:
        logger.info(result.stdout.strip())


def ensure_mount(logger):
    if SOURCE_MODE != "mounted":
        logger.info("Mount check skipped (SOURCE_MODE=%s).", SOURCE_MODE)
        return

    if not REQUIRE_MOUNT:
        logger.info("Mount check disabled via DBF_REQUIRE_MOUNT.")
        return

    if MOUNT_POINT.exists() and os.path.ismount(MOUNT_POINT):
        logger.info("Network drive mounted: %s", MOUNT_POINT)
        return

    MOUNT_POINT.mkdir(parents=True, exist_ok=True)

    if SMB_USER and SMB_PASS:
        smb_url = SMB_URL.replace("smb://", f"smb://{SMB_USER}:{SMB_PASS}@", 1)
    else:
        smb_url = SMB_URL

    cmd = ["mount_smbfs", smb_url, str(MOUNT_POINT)]
    logger.info("Mounting network drive: %s", MOUNT_POINT)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "mount_smbfs failed")


def main():
    parser = argparse.ArgumentParser(
        description="Mirror remote DBF folder locally and sync deltas daily."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force a sync even if it already ran today.",
    )
    args = parser.parse_args()

    logger = setup_logger()
    logger.info("----- DBF DELTA STARTED -----")

    state = load_state()
    today = datetime.today().date().isoformat()

    if state.get("last_sync_date") == today and not args.force:
        logger.info("Already synced today (%s). Use --force to rerun.", today)
        return

    initial_sync = "initial_sync_date" not in state
    logger.info("Initial sync: %s", "yes" if initial_sync else "no")

    try:
        ensure_mount(logger)
        run_rsync(logger)
        state["last_sync_date"] = today
        if initial_sync:
            state["initial_sync_date"] = today
        save_state(state)
        logger.info("Sync completed.")
    except Exception as e:
        logger.error("Sync failed: %s", e)
    finally:
        logger.info("----- DBF DELTA FINISHED -----")


if __name__ == "__main__":
    main()
