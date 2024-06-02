from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# Load environment variables from .env file if it exists
load_dotenv()

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
logger.info(f"PROJ_ROOT path is: {PROJ_ROOT}")

DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

SCHEDULED_EVENTS = RAW_DATA_DIR / "scheduled_events"
SCHEDULED_EVENTS_JSON = SCHEDULED_EVENTS / "json"
SCHEDULED_EVENTS_CSV = SCHEDULED_EVENTS / "csv"

ODDS = RAW_DATA_DIR / "odds"
ODDS_JSON = ODDS / "json"
ODDS_CSV = ODDS / "csv"

STATISTICS = RAW_DATA_DIR / "statistics"
STATISTICS_JSON = STATISTICS / "json"
STATISTICS_CSV = STATISTICS / "csv"

INCIDENTS = RAW_DATA_DIR / "INCIDENTS"
INCIDENTS_JSON = INCIDENTS / "json"
INCIDENTS_CSV = INCIDENTS / "csv"

LINEUPS = RAW_DATA_DIR / "lineups"
LINEUPS_JSON = LINEUPS / "json"
LINEUPS_CSV = LINEUPS / "csv"

MODELS_DIR = PROJ_ROOT / "models"

REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

# If tqdm is installed, configure loguru with tqdm.write
# https://github.com/Delgan/loguru/issues/135
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass
