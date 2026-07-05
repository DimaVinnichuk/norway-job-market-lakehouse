from src.ingest import generate_history_point, get_jwt_token, \
    get_start_date, ingest_feed_data, ingest_job_details, generate_header_data
from logging_config import setup_logging
import logging
from datetime import datetime, timezone
from src.constants import BRONZE_FEED_DIR, STATE_FILE, ACTIVE_JOB_DIR, INACTIVE_JOB_DIR

setup_logging()
logger = logging.getLogger("__name__")

def run_feed_ingestion():
    """Launch feed ingestion process"""

    logger.info("Pipeline start.")

    token = get_jwt_token()
    state_file_content = get_start_date(generate_history_point, STATE_FILE)
    header_data = generate_header_data(token, state_file_content)
    current_date = datetime.now(timezone.utc)

    incomplited_run_dir = None
    if BRONZE_FEED_DIR.exists():
        state_date = datetime.strptime(state_file_content, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
        dirs = list(BRONZE_FEED_DIR.glob("run-*"))
        if dirs:
            dir_date = None
            for dir in dirs:
                run_files = list(dir.glob("*.json"))
                if not run_files:
                    logger.debug("Empty dir %s removed", dir)
                    dir.rmdir()
                    continue
                date_from_dir = datetime.strptime(dir.name.strip(), "run-%d%m%y-%H%M%S").replace(tzinfo=timezone.utc)
                if date_from_dir > state_date:
                    incomplited_run_dir = dir
                    dir_date = date_from_dir
                    
    if incomplited_run_dir:
        logger.info("Found incomplited session folder: %s Start emergency ingestion", incomplited_run_dir)
        ingest_job_details(dir_date, incomplited_run_dir, ACTIVE_JOB_DIR, INACTIVE_JOB_DIR, header_data, STATE_FILE)
        
        pipeline_run_date = dir_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
        STATE_FILE.write_text(pipeline_run_date, encoding="utf-8")
        logger.info("Pipeline state file updated with %s", pipeline_run_date)
    else:
        logger.info("Default ingestion")
        fresh_feed_dir = ingest_feed_data(current_date, header_data, BRONZE_FEED_DIR)
        ingest_job_details(current_date, fresh_feed_dir, ACTIVE_JOB_DIR, INACTIVE_JOB_DIR, header_data, STATE_FILE)

        pipeline_run_date = current_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
        STATE_FILE.write_text(pipeline_run_date, encoding="utf-8")
        logger.info("Pipeline state file updated with %s", pipeline_run_date)

    logger.info("Pipeline successfuly ended.")

if __name__ == "__main__":
    run_feed_ingestion()