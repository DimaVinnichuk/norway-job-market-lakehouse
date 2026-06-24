import json
from pathlib import Path
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def get_ids_active_vacancies(feed_dir, vacancies_state_file, start_date):
    """Define, write and update vacancies states in file"""

    logger.info("Updating vacancies status")
    feed_elements = feed_dir.glob("*.json")
    pipeline_run_date = datetime.strptime(start_date, "%a, %d %b %Y %H:%M:%S GMT")
    pipeline_run_date = pipeline_run_date.replace(tzinfo=timezone.utc)
    states = {}

    if vacancies_state_file.exists():
        with open(vacancies_state_file, "r", encoding="utf-8") as f:
            try:
                states = json.load(f)
            except json.JSONDecodeError as e:
                logger.critical("File %s contains not valid json. Error %s", vacancies_state_file, e)
        logger.info("Vacancies state file found at %s. Updating data", vacancies_state_file)
    else:
        vacancies_state_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Vacancies state file not found. New file created: %s", vacancies_state_file)

    for feed_element in feed_elements:
        file_modif_date = datetime.fromtimestamp(feed_element.stat().st_mtime, tz=timezone.utc)
        if file_modif_date <= pipeline_run_date:
            continue
        with open(feed_element, "r", encoding="utf-8") as f:
            try:
                feed_obj = json.load(f)
            except json.JSONDecodeError as e:
                logger.critical("File %s skipped. Not valid json. Error %s", feed_element, e)
                continue

            for item in feed_obj.get("items", []):
                feed_entry = item.get("_feed_entry")
                if not feed_entry:
                    logger.warning("Vacancy %s skipped. '_feed_entry' is empty", item.get("id"))
                else:
                    st = {f"{item.get("id")}": f"{feed_entry.get("status")}"}
                    states.update(st)
                
    with open(vacancies_state_file, "w", encoding="utf-8") as f:
        json.dump(states, f, indent=4)
    logger.info("Update finished. Data saved at %s", vacancies_state_file)