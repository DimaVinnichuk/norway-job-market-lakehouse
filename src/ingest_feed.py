from datetime import datetime, timezone, timedelta
import logging
import requests
import json
from urllib.parse import urljoin
import time

logger = logging.getLogger(__name__)

URL_TOKEN = "https://pam-stilling-feed.nav.no/api/publicToken"
URL_FEED = "https://pam-stilling-feed.nav.no/api/v1/feed"
DOMAIN = "https://pam-stilling-feed.nav.no/"

def generate_history_point():
    """Creates a timestamp for 30 days ago, which will be the starting point for populating historical data """

    logger.debug("Creating historical date point...")
    today = datetime.now(timezone.utc)
    thirty_days_ago = (today - timedelta(days=30)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    logger.debug("Created date point: %s", thirty_days_ago)
    return thirty_days_ago

def get_pipeline_status(get_date, state_file):
    """Checks for pipeline status file existence, reading its data or falling back to default."""

    logger.info("Checking pipeline execution status...")
    if state_file.exists():
        logger.info("Pipeline status file found at %s. Reading existing state.", state_file)
        content = state_file.read_text(encoding="utf-8").strip()
        logger.debug("Retrieved date from status file: %s", content)
        return content
    else:
        logger.info("Pipeline status file NOT found at %s. Triggering historical backfill mode.", state_file)
        backfill_point = get_date()
        return backfill_point

def get_jwt_token():
    """Requests and returns a public token from the NAV API"""

    try:
        logger.info("Requesting public token...")
        token_response = requests.get(URL_TOKEN, timeout=10)
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
            raise SystemExit(1)

    except requests.exceptions.RequestException as e:
        logger.critical("Error retrieving token: %s.", e)
        raise SystemExit(1)

def fetch_and_save_feed_data(token, time_point, bronze_feed_dir, state_file):
    """Fetch and save feed data"""

    header_data = {
        "Authorization": f"Bearer {token}",
        "If-Modified-Since": f"{time_point}",
        "Accept": "application/json"
    }

    logger.info("Fetching vacancy feed data from API...")
    start_timestamp = datetime.now(timezone.utc)
    timestamp_filename = start_timestamp.strftime("%d%m%y-%H%M%S")
    timestamp_for_update_state = start_timestamp.strftime("%a, %d %b %Y %H:%M:%S GMT")
    page_url = URL_FEED
    page_number = 1

    while page_url:
        data_filename = f"{timestamp_filename}-page{page_number}-raw_jobs.json"
        path_to_data = bronze_feed_dir / data_filename
        path_to_data.parent.mkdir(parents=True, exist_ok=True)

        try:
            data_json = None
            # logger.debug("Requesting URL: %s...", page_url)
            response = requests.get(page_url, headers=header_data, timeout=10)
            response.raise_for_status()
            try:
                data_json = response.json()
                # logger.debug("Response content parsed to json successfuly")
            except json.JSONDecodeError as e:
                logger.critical("Failed to parse response content to json. Error: %s", e)
                raise SystemExit(1)
        except requests.exceptions.RequestException as e:
            logger.critical("Failed to retrieve data from API: %s.", e)
            raise SystemExit(1)
        
        if data_json.get("items"):
            with open(path_to_data, "w", encoding="utf-8") as f:
                json.dump(data_json, f, indent=4)
            # logger.debug("Retrieved data saved to %s", path_to_data)
            page_number+=1
            new_url = None
            next_url = data_json.get("next_url")
            if next_url:
                new_url = urljoin(DOMAIN, next_url)
            else:
                logger.info("Next URL is missing. Pagination finished.")
                break
            if new_url != page_url:
                page_url = new_url
                # logger.debug("Next page URL: %s", page_url)
            else:
                logger.critical("Next URL is equal to past one. Validation failed")
                break        
        else:
            logger.info("No vacancy data found in the response")
            break
        time.sleep(1)

    logger.info("Vacancy data ingestion completed successfully.")
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(timestamp_for_update_state, encoding="utf-8")
    logger.debug("State file created/updated, at: %s", state_file)
    logger.debug("Pipeline state file updated with timestamp: %s", timestamp_for_update_state)
