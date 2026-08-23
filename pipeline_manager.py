from src.ingest import generate_history_point, get_jwt_token, \
    get_start_date, ingest_feed_data, ingest_job_details, generate_header_data
from logging_config import setup_logging
import logging
from datetime import datetime, timezone
from src.constants import BRONZE_FEED_DIR, STATE_FILE, JOB_DETAILS_DIR

setup_logging()
logger = logging.getLogger("__name__")

def run_feed_ingestion():
    """Launch feed ingestion process"""

    logger.info("Pipeline start.")

    token = get_jwt_token()
    state_file_content = get_start_date(generate_history_point, STATE_FILE)
    header_data = generate_header_data(token, state_file_content)
    current_date = datetime.now(timezone.utc)
    feed_ingest_complited = JOB_DETAILS_DIR.exists()

    logger.debug("Checking for failed feed data ingestion session")
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
                else:
                    if not feed_ingest_complited:
                        logger.debug("Found first failed pipeline start. Run dir and data removed.")
                        for data in run_files:
                            data.unlink()
                        dir.rmdir()
                    if feed_ingest_complited and run_files:
                        date_from_dir = datetime.strptime(dir.name.strip(), "run-%d%m%y-%H%M%S").replace(tzinfo=timezone.utc)
                        if date_from_dir > state_date:
                            incomplited_run_dir = dir
                            dir_date = date_from_dir
                    
    if incomplited_run_dir:
        logger.info("Found incomplited session folder: %s Resuming previous feed ingestion session", incomplited_run_dir)
        ingest_job_details(dir_date, incomplited_run_dir, JOB_DETAILS_DIR, header_data)
        
        pipeline_run_date = dir_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
        STATE_FILE.write_text(pipeline_run_date, encoding="utf-8")
        logger.info("Pipeline state file updated with %s", pipeline_run_date)
    else:
        logger.debug("No previous failed feed ingestion session was found")
        fresh_feed_dir = ingest_feed_data(current_date, header_data, BRONZE_FEED_DIR)
        ingest_job_details(current_date, fresh_feed_dir, JOB_DETAILS_DIR, header_data)

        pipeline_run_date = current_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
        STATE_FILE.write_text(pipeline_run_date, encoding="utf-8")
        logger.info("Pipeline state file updated with %s", pipeline_run_date)

    logger.info("Pipeline successfuly ended.")

if __name__ == "__main__":
    run_feed_ingestion()