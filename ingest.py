from datetime import datetime, timezone, timedelta
from pathlib import Path
from logging_config import setup_logging
import logging
import requests
import json

setup_logging()
logger = logging.getLogger(__name__)

URL_TOKEN = "https://pam-stilling-feed.nav.no/api/publicToken"
URL_VACANCY = "https://pam-stilling-feed.nav.no/api/v1/feed"

STATUS_FILE_PATH = Path("data/pipeline_state.txt")

def generate_history_point():
    """Creates a timestamp for 30 days ago, which will be the starting point for populating historical data """

    logger.debug("Creating historical date point...")
    today = datetime.now(timezone.utc)
    thirty_days_ago = (today - timedelta(days=30)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    logger.debug("Created date point: %s", thirty_days_ago)
    return thirty_days_ago

def get_pipeline_status(file_path, get_date):
    """Checks for pipeline status file existence, reading its data or falling back to default."""

    logger.info("Checking pipeline execution status...")
    if file_path.exists():
        logger.info("Pipeline status file found at %s. Reading existing state.", file_path)
        content = file_path.read_text(encoding="utf-8").strip()
        logger.debug("Retrieved date from status file: %s", content)
        return content
    else:
        logger.info("Pipeline status file NOT found at %s. Triggering historical backfill mode.", file_path)
        backfill_point = get_date()
        return backfill_point

def get_jwt_token(url_token):
    """Requests and returns a public token from the NAV API"""

    try:
        logger.info("Requesting public token...")
        token_response = requests.get(url_token, timeout=10)
        token_response.raise_for_status()
        try:
            token = token_response.text.split(":")[-1].strip()
            if (len(token.split(".")) == 3):
                logger.info("Public token successfully retrieved.")
                return token
            else: 
                raise ValueError("Invalid JWT token structure received from API")
        except ValueError as e:
            logger.critical("Token parsing error %s", e)
            raise SystemExit(e)

    except requests.exceptions.RequestException as e:
        logger.critical("Error retrieving token: %s.", e)
        raise SystemExit(e)

def get_vacancy_data(url, header):
    # TODO: Finalize data extraction logic to fetch full-size data. Add validation.
    """Requests and returns JSON data about vacancies."""

    logger.info("Requesting API to retrieve data...")
    try:
        response = requests.get(url, headers=header)
        response.raise_for_status()
        json_vacancy_data = response.json()

        logger.info("Data successfully retrieved.")
        return json_vacancy_data
    except requests.exceptions.RequestException as e:
        logger.critical("Failed to retrieve data from API: %s.", e)
        raise SystemError(e)

def save_vacancy_data(vacancy_data):
    # TODO: Implement saving each data page into a separate file.
    """Saves vacancy JSON data to a file"""

    path_to_file = Path("data/bronze/vacancy_data.json")
    path_to_file.parent.mkdir(parents=True, exist_ok=True)

    with open(path_to_file, "w", encoding="utf-8") as f:
        json.dump(vacancy_data, f, indent=4)

    if path_to_file.exists:
        logger.info("Data successfully saved")
    else:
        logger.error("Error: Failed to create data file")

### main execution

def run_ingestion():
    """Launch ingestion process"""

    logger.info("Start ingestion process")
    defined_datetime_point = get_pipeline_status(STATUS_FILE_PATH, generate_history_point)
    jwt_token = get_jwt_token(URL_TOKEN)
    header_data = {
        "Authorization": f"Bearer {jwt_token}",
        "If-Modified-Since": f"{defined_datetime_point}"
    }

    result = get_vacancy_data(URL_VACANCY, header_data)
    save_vacancy_data(result)

    logger.info("End ingestion process")


if __name__ == "__main__":
    run_ingestion()