from src.ingest_feed import generate_history_point, \
    get_jwt_token, get_pipeline_status, fetch_and_save_feed_data
from logging_config import setup_logging
import logging
from src.constants import BRONZE_FEED_DIR, STATE_FILE, VACANCIES_STATUS_FILE
from src.ingest_details import get_ids_active_vacancies

setup_logging()
logger = logging.getLogger("__name__")

def run_feed_ingestion():
    """Launch feed ingestion process"""

    logger.info("Start feed ingestion process.")
    start_date_point = get_pipeline_status(generate_history_point, STATE_FILE)
    jwt_token = get_jwt_token()
    fetch_and_save_feed_data(jwt_token, start_date_point, BRONZE_FEED_DIR, STATE_FILE)
    get_ids_active_vacancies(BRONZE_FEED_DIR, VACANCIES_STATUS_FILE, start_date_point)
    logger.info("End feed ingestion process.")

if __name__ == "__main__":
    run_feed_ingestion()