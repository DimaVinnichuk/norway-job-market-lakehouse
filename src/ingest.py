from datetime import datetime, timezone, timedelta
import logging
import requests
import json
from urllib.parse import urljoin
import time
from pathlib import Path

logger = logging.getLogger(__name__)

URL_TOKEN = "https://pam-stilling-feed.nav.no/api/publicToken"
URL_FEED = "https://pam-stilling-feed.nav.no/api/v1/feed"
URL_DETAILS = "https://pam-stilling-feed.nav.no/api/v1/feedentry/"
DOMAIN = "https://pam-stilling-feed.nav.no/"

def generate_header_data(token, str_time_point):
    """Generate header data for API request"""

    head = {
        "Authorization": f"Bearer {token}",
        "If-Modified-Since": f"{str_time_point}",
        "Accept": "application/json"
    }
    return head

def generate_history_point():
    """Creates a timestamp for 30 days ago, which will be the starting point for populating historical data """

    logger.debug("Creating historical date point...")
    today = datetime.now(timezone.utc)
    thirty_days_ago = (today - timedelta(days=30)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    logger.debug("Created date point: %s", thirty_days_ago)
    return thirty_days_ago

def get_start_date(generate_history_point, state_file):
    """Checks for pipeline state file existence, reading its data or falling back to default."""
    
    if state_file.exists():
        logger.info("Pipeline state file found at %s. Reading existing state.", state_file)
        content = state_file.read_text(encoding="utf-8").strip()
        logger.debug("Retrieved date from state file: %s", content)
        return content
    else:
        logger.info("Pipeline status file NOT found at %s. Triggering historical backfill mode.", state_file)
        backfill_point = generate_history_point()
        return backfill_point

def get_jwt_token():
    """Requests and returns a public token from the NAV API"""

    try:
        logger.info("Requesting public token...")
        token_response = requests.get(URL_TOKEN, timeout=10)
        token_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.critical("Error retrieving token: %s.", e)
        raise SystemExit(1)
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

def ingest_feed_data(pipeline_run_date, header_data, bronze_feed_dir):
    """Extract and feed data and return curent run dir path"""

    logger.info("Ingesting feed data from API...")
    timestamp_filename = pipeline_run_date.strftime("%d%m%y-%H%M%S")
    current_run_dir = bronze_feed_dir / f"run-{timestamp_filename}"
    current_run_dir.mkdir(parents=True, exist_ok=True)
        
    page_url = URL_FEED
    page_number = 1

    while page_url:
        filename = f"{timestamp_filename}-page{page_number}-raw-jobs.json"
        path_to_data = current_run_dir / filename
        try:
            data_json = None
            response = requests.get(page_url, headers=header_data, timeout=10)
            if response.status_code == 404:
                logger.info("No fresh data in feed. Response status code: 404")
                logger.info("Feed data ingestig proccess ended")
                raise SystemExit(1)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.critical("Failed to retrieve data from API: %s.", e)
            raise SystemExit(1)
        try:
            data_json = response.json()
        except json.JSONDecodeError as e:
            logger.critical("Failed to parse response content to json. Error: %s", e)
            raise SystemExit(1)
        with open(path_to_data, "w", encoding="utf-8") as f:
            json.dump(data_json, f, indent=4)
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
        else:
            logger.critical("Next URL is equal to past one. Validation failed")
            break
        time.sleep(0.5)
    logger.info("Feed data ingestion completed successfully.")
    return current_run_dir

def ingest_job_details(dirs_name_date, fresh_feed_data_dir, active_dir, inactive_dir, header_data, pipeline_state_file):
    """Extract and save job details from API"""

    def write_data_to_file(mode, data, batch_num, partition):
        filename = f"{dir_time}_batch{batch_num}.json"
        filepath = partition / filename
        with open(filepath, mode, encoding="utf-8")as f:
            json.dump(data, f, indent=4)
        return batch_num + 1
    
    active_batch_num = 1
    inactive_batch_num = 1

    partition = Path(f"{dir_year}/{dir_month}/{dir_day}")
    active_partition_path = active_dir / partition
    inactive_partition_path = inactive_dir / partition

    writed_job_ids = None

    if active_partition_path.exists():
        active_jobs_batches = list(active_partition_path.glob("*.json"))
        if active_jobs_batches:
            active_batch_num = len(active_jobs_batches)
            for active_job_batch in active_jobs_batches:
                for job in active_job_batch:
                    job_id = job.get("uuid")
                    if job_id:
                        writed_job_ids.add()
                    else:
                        logger.warning("Missed uuid for job in batch %s", active_job_batch)
        else:
            active_partition_path.rmdir()
            logger.info("Empty path %s, removed", active_partition_path)
            


    logger.info("Ingesting job details...")
    logger.debug("Reading fresh feed data dir: %s...", fresh_feed_data_dir)

    feed_files = list(fresh_feed_data_dir.glob("*.json"))
    if fresh_feed_data_dir.exists() and not feed_files:
        fresh_feed_data_dir.rmdir()
        logger.debug("Empty directory %s cleaned", fresh_feed_data_dir)
        logger.info("No json data files found at %s. Job details ingestion finished", fresh_feed_data_dir)
        raise SystemExit(1)
    
    logger.debug("Feed data found at: %s", fresh_feed_data_dir)
    logger.debug("Creating unique ids from fresh feed data for request details")
    uniq_ids = set()
    for feed_file in feed_files:
        with open(feed_file, "r", encoding="utf-8") as f:
            try:
                feed_file_data = json.load(f)
            except json.JSONDecodeError as e:
                logger.warning("Skip file %s. Do not contains valid json. Error: %s", feed_file, e)
                continue
            feed_items = feed_file_data.get("items")
            if feed_items:
                for item in feed_items:
                    item_id = item.get("id").strip()
                    if not item_id:
                        logger.warning("Vacancy from file %s skipped. Not valid id", feed_file)
                    else:
                        uniq_ids.add(item_id)
            else:
                logger.warning("Skip file %s. Feed file do not contains any job data.", feed_file)
    logger.info(f"{len(uniq_ids)} new changes in feed was found.")

    dir_year = dirs_name_date.strftime("%Y")
    dir_month = dirs_name_date.strftime("%m")
    dir_day = dirs_name_date.strftime("%d")
    dir_time = dirs_name_date.strftime("%H%M%S")

    inactive_partition_path.mkdir(parents=True, exist_ok=True)
    active_partition_path.mkdir(parents=True, exist_ok=True)

    active_job_list = []
    inactive_job_list = []

    for job_id in uniq_ids:
        url_details = urljoin(URL_DETAILS, job_id)
        try:
            response = requests.get(url_details, headers=header_data, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.critical("Failed to retrieve data details from API: %s", e)
            raise SystemExit(1)
        try:
            job_details = response.json()
        except json.JSONDecodeError as e:
            logger.warning("Skippeed job details for id: %s. Error: %s", job_id, e)
            continue

        job_status = job_details.get("status").strip()
        if job_status:
            if job_status == "ACTIVE":
                active_job_list.append(job_details)
                if len(active_job_list) >= 100:
                    active_batch_num = write_data_to_file("w", active_job_list, active_batch_num, active_partition_path)
                    active_job_list.clear()
            else:
                inactive_job_list.append(job_details)
                if len(inactive_job_list) >= 1000:
                    inactive_batch_num = write_data_to_file("w", inactive_job_list, inactive_batch_num, inactive_partition_path) 
                    inactive_job_list.clear()
        else:
            logger.warning("Job details respons has no status fiels. ID: %s skipped", job_id)      
    time.sleep(0.5)

    if active_job_list:
        write_data_to_file("w", active_job_list, active_batch_num, active_partition_path)
    if inactive_job_list:
        write_data_to_file("w", inactive_job_list, inactive_batch_num, inactive_partition_path)
    logger.info("End ingesting job details proccess.")


    