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
    thirty_days_ago = (today - timedelta(hours=24)).strftime("%a, %d %b %Y %H:%M:%S GMT")
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
        logger.info("Retrieving public token...")
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
    """Extract and save feed data. Return curent run dir path with saved feed data"""

    logger.info("Ingesting feed data from API...")
    timestamp_filename = pipeline_run_date.strftime("%d%m%y-%H%M%S")
    current_run_dir = bronze_feed_dir / f"run-{timestamp_filename}"
    current_run_dir.mkdir(parents=True, exist_ok=True)
        
    page_url = URL_FEED
    page_number = 0
    items_number = 0

    while page_url:
        filename = f"{timestamp_filename}-page{page_number}-raw-jobs.json"
        path_to_data = current_run_dir / filename
        try:
            data_json = None
            response = requests.get(page_url, headers=header_data, timeout=10)
            if response.status_code == 404:
                logger.info("No fresh data in feed. Response status code: 404")
                logger.info("Feed data ingesting proccess completed")
                logger.info("Pipeline completed")
                raise SystemExit(1)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.critical("Failed to retrieve data from API: %s.", e)
            raise SystemExit(1)
        try:
            data_json = response.json()
            items = data_json.get("items")
            if items:
                items_number += len(items)
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
            logger.info("%s page(s) and %s item(s) from feed was saved at %s", page_number, items_number, current_run_dir)
            break
        if new_url != page_url:
            page_url = new_url
        else:
            logger.critical("Next URL is equal to past one. Validation failed")
            break
        time.sleep(0.5)
    logger.info("Feed data ingestion completed.")
    return current_run_dir

def ingest_job_details(dirs_name_date, fresh_feed_data_dir, active_dir, inactive_dir, header_data):
    """Extract and save job details from API"""

    def write_data_to_file(mode, data, batch_num, partition):
        """Write prepared batch to a file"""

        filename = f"{dir_time}_batch{batch_num}.json"
        filepath = partition / filename
        with open(filepath, mode, encoding="utf-8")as f:
            json.dump(data, f, indent=4)
        return batch_num + 1
    
    def check_already_saved_ids (data_path, dir_time):
        """Return set of already saved job details from last incomplited session if exists and number of butch to correct batch counting"""

        logger.debug("Checking for already saved active/inactive job details")
        batch_num = 1
        writed_job_ids = set()
        if data_path.exists():
            job_batches = list(data_path.glob(f"{dir_time}_batch*.json"))
            if job_batches:
                logger.debug("Already saved job details found at %s", data_path)
                batch_num = len(job_batches) + 1
                for job_batch in job_batches:
                    with open(job_batch, "r", encoding="utf-8") as f:
                        try:
                            job_batch_json = json.load(f)
                            for job in job_batch_json:
                                job_id = job.get("uuid")
                                if job_id:
                                    writed_job_ids.add(job_id)
                                else:
                                    logger.warning("Missed uuid for job in batch %s", job_batch)
                        except json.JSONDecodeError as e:
                            logger.warning("Not valid json. Skipped batch %s. Error: %s", job_batch, e)
                        
                return writed_job_ids, batch_num
            else:
                logger.debug("Allready saved job details not found at %s", data_path)
                return writed_job_ids, batch_num
        else:
            logger.debug("Directory %s is empty", data_path)
            return writed_job_ids, batch_num
    
    dir_year = dirs_name_date.strftime("%Y")
    dir_month = dirs_name_date.strftime("%m")
    dir_day = dirs_name_date.strftime("%d")
    dir_time = dirs_name_date.strftime("%H%M%S")

    partition = Path(f"{dir_year}/{dir_month}/{dir_day}")
    active_partition_path = active_dir / partition
    inactive_partition_path = inactive_dir / partition

    inactive_partition_path.mkdir(parents=True, exist_ok=True)
    active_partition_path.mkdir(parents=True, exist_ok=True)

    active_saved_job_ids, active_batch_num = check_already_saved_ids(active_partition_path, dir_time)
    inactive_saved_job_ids, inactive_batch_num = check_already_saved_ids(inactive_partition_path, dir_time)

    logger.info("Ingesting job details...")
    logger.debug("Reading fresh feed data dir: %s...", fresh_feed_data_dir)

    feed_files = list(fresh_feed_data_dir.glob("*.json"))
    if fresh_feed_data_dir.exists() and not feed_files:
        fresh_feed_data_dir.rmdir()
        logger.debug("Empty directory %s cleaned", fresh_feed_data_dir)
        logger.info("No json data files found at %s. Job details ingestion finished", fresh_feed_data_dir)
        raise SystemExit(1)
    
    logger.debug("Feed data found at: %s", fresh_feed_data_dir)
    logger.debug("Creating unique ids from fresh feed data for request job details")
    uniq_ids = set()
    for feed_file in feed_files:
        with open(feed_file, "r", encoding="utf-8") as f:
            try:
                feed_file_data = json.load(f)
            except json.JSONDecodeError as e:
                logger.warning("Skipped file %s. Do not contains valid json. Error: %s", feed_file, e)
                continue
            feed_items = feed_file_data.get("items")
            if feed_items:
                for item in feed_items:
                    item_id = item.get("id").strip()
                    if not item_id:
                        logger.warning("Skipped job from file: %s. Not valid id field", feed_file)
                    else:
                        uniq_ids.add(item_id)
            else:
                logger.warning("Skipped file: %s. Feed file do not contains any job data.", feed_file)
    logger.info(f"{len(uniq_ids)} uniq job id(s) was retrieved from feed data.")

    if active_saved_job_ids or inactive_saved_job_ids : logger.info("Resuming failed ingestion job details session.")

    if active_saved_job_ids:
        logger.info("%s allready saved active ids skipped.", len(active_saved_job_ids))
        uniq_ids.difference_update(active_saved_job_ids)

    if inactive_saved_job_ids:
        logger.info("%s allready saved inactive ids skipped.", len(inactive_saved_job_ids))
        uniq_ids.difference_update(inactive_saved_job_ids)
    
    active_job_list = []
    inactive_job_list = []
    job_details_number = 0

    logger.debug("Requesting job details by retrieved ids...")
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
            logger.warning("Skipped job details for id: %s. Error: %s", job_id, e)
            continue

        job_status = job_details.get("status").strip()
        if job_status:
            job_details_number += 1
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
            logger.warning("Job details response has no status field. Skipped ID: %s", job_id)      
    time.sleep(0.5)

    if active_job_list:
        write_data_to_file("w", active_job_list, active_batch_num, active_partition_path)
    if inactive_job_list:
        write_data_to_file("w", inactive_job_list, inactive_batch_num, inactive_partition_path)
    logger.info("%s job details was saved at: %s and(or) %s", job_details_number, active_partition_path, inactive_partition_path)
    logger.info("Job details ingesting completed.")


    