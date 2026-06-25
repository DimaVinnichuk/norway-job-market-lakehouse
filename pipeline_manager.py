from src.ingest import generate_history_point, get_jwt_token, \
    get_pipeline_status, fetch_and_save_feed_data, fetch_and_save_vacancy_details, generate_header_data
from logging_config import setup_logging
import logging
from src.constants import BRONZE_FEED_DIR, STATE_FILE, ACTIVE_VACANCY_DETAILS_DIR, INACTIVE_VACANCY_DETAILS_DIR

setup_logging()
logger = logging.getLogger("__name__")

def run_feed_ingestion():
    """Launch feed ingestion process"""

    logger.info("Start ingestion process.")
    last_run_date = get_pipeline_status(generate_history_point, STATE_FILE)
    jwt_token = get_jwt_token()
    header_data = generate_header_data(jwt_token, last_run_date)
    fetch_and_save_feed_data(header_data, BRONZE_FEED_DIR, STATE_FILE)
    fetch_and_save_vacancy_details(BRONZE_FEED_DIR, ACTIVE_VACANCY_DETAILS_DIR, INACTIVE_VACANCY_DETAILS_DIR, last_run_date, header_data)
    logger.info("End ingestion process.")

if __name__ == "__main__":
    run_feed_ingestion()