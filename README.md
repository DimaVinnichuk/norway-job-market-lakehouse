# End-to-End AI-Driven Data Platform: Norwegian Job Market Analytics

## About the Project (Business Case)

This project addresses the business challenge of analyzing unstructured textual data in the Norwegian job market. The state system (NAV) collects thousands of job postings daily, but they are provided in a chaotic format (raw HTML), complicating direct analytics for HR or consulting companies.

The goal of the project is to build an incremental data processing pipeline (Medallion Architecture) that collects raw texts, cleans them, and uses Artificial Intelligence (LLM) to extract hidden metrics such as technical skills, language requirements, and seniority levels. This enables the creation of an analytical data mart for monitoring IT trends in real-time.

## Architecture (Medallion Architecture)

The project is built around the modern cloud Lakehouse model.

* **Bronze (Ingestion):** Automated data collection from the public NAV API using Python. Data is stored as raw JSON files and partitioned by active and inactive job statuses.


* **Silver (Cleansing - In Progress):** Using Azure Databricks (PySpark) for deduplication, geography normalization, and IT industry filtering. Data will be stored in Delta Lake format.


* **Gold + AI (Enrichment - In Progress):** Integration with the OpenAI API for entity extraction (identifying Hard/Soft skills, Seniority Level).


* **BI (Visualization - In Progress):** Interactive Power BI dashboard for "Time-to-Hire" and "Market Intelligence" analysis.



## Technology Stack

* **Programming Languages:** Python, SQL.


* **Cloud & Infrastructure (IaC):** Microsoft Azure, Terraform (planned).


* **Orchestration:** GitHub Actions.


* **Data Processing (Compute & Storage):** Azure Databricks (PySpark, Delta Lake).


* **AI Integration:** OpenAI API.



## Current Project Status (Implemented Features)

Currently, the Bronze layer (Ingestion pipeline) is fully implemented:

* Developed a modular Python client to interact with the NAV API (pam-stilling-feed).


* Implemented automated JWT token retrieval and pagination handling.


* Configured incremental data loading using a state file (`pipeline_state.txt`), which allows the pipeline to download only new or modified data since the last execution.


* Job postings are parsed and saved in batches into respective directories, categorized by `ACTIVE` and `INACTIVE` statuses.


* Integrated a robust logging system (file rotation, console output) to monitor pipeline execution.


* Implemented a session recovery mechanism to resume interrupted script executions by checking for incomplete directory runs.



## How to Run Locally (Bronze Ingestion)

1. **Clone the repository:**
```bash
git clone <your-repo-url>
cd <your-repo-name>

```


2. **Install dependencies:**
The script relies on standard HTTP client libraries and character encoding handlers.


```bash
pip install -r requirements.txt

```


3. **Run the pipeline:**
The script will automatically check for the pipeline state file. If it is missing, it will trigger a historical data backfill starting from 30 days ago.


```bash
python pipeline_manager.py

```


4. **Logs and Data:**
* Execution logs can be found in `data/logs/pipeline.log`.


* Downloaded data batches will be stored within the `data/bronze/` directory structure.