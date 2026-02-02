import requests
import json
from datetime import datetime
from snowflake.snowpark import Session
import sys
import pytz
import logging
from requests.adapters import HTTPAdapter, Retry
import os
import time

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# -----------------------------
# Timezone setup
# -----------------------------
ist_timezone = pytz.timezone('Asia/Kolkata')
current_time_ist = datetime.now(ist_timezone)
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')
today_string = current_time_ist.strftime('%Y_%m_%d')
file_name = f'air_quality_data_{timestamp}.json'

# -----------------------------
# Snowflake connection
# -----------------------------
def snowpark_basic_auth() -> Session:
    connection_parameters = {
        "ACCOUNT": "IBJXEPF-NV18928",
         "USER": os.environ["SNOWFLAKE_USER"], 
        "PASSWORD": os.environ["SNOWFLAKE_PASSWORD"],
        "ROLE": "SYSADMIN",
        "DATABASE": "dev_db",
        "SCHEMA": "stage_sch",
        "WAREHOUSE": "load_wh"
    }
    try:
        logging.info("Connecting to Snowflake...")
        session = Session.builder.configs(connection_parameters).create()
        logging.info("Connected to Snowflake successfully")
        return session
    except Exception as e:
        logging.error(f"Snowflake connection failed: {e}")
        sys.exit(1)

# -----------------------------
# REST API ingestion with retries & backoff
# -----------------------------
def get_air_quality_data(api_key, limit, max_retries=5):
    api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'

    params = {
        'api-key': api_key,
        'format': 'json',
        'limit': limit
    }

    headers = {
        'accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (compatible; StudyProject/1.0; +https://github.com/Abc123)'
    }

    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    attempt = 0
    while attempt < max_retries:
        try:
            logging.info(f"API request attempt {attempt + 1}")
            response = session.get(api_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()  # raise for HTTP errors
            logging.info("API response received successfully")
            json_data = response.json()

            # Write JSON locally
            with open(file_name, 'w') as f:
                json.dump(json_data, f, indent=2)
            logging.info(f"Saved JSON data to {file_name}")

            # Upload to Snowflake
            # stg_location = f'@dev_db.stage_sch.raw_stg/india/{today_string}/'
            stg_location = '@dev_db.stage_sch.raw_stg/india/'+today_string+'/'
            sf_session = snowpark_basic_auth()
            logging.info(f"Uploading {file_name} to Snowflake stage {stg_location}")
            sf_session.file.put(file_name, stg_location, auto_compress=True, overwrite=True)

            lst_query = f'LIST {stg_location}{file_name}.gz'
            result_lst = sf_session.sql(lst_query).collect()
            logging.info(f"File in stage: {result_lst}")
            sf_session.close()
            logging.info("Snowflake session closed")

            return json_data

        except requests.exceptions.RequestException as e:
            logging.warning(f"API request failed on attempt {attempt+1}: {e}")
            attempt += 1
            wait_time = 2 ** attempt  # exponential backoff
            logging.info(f"Waiting {wait_time} seconds before retry")
            time.sleep(wait_time)
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            sys.exit(1)

    logging.error("Max retries exceeded. Exiting.")
    sys.exit(1)

# -----------------------------
# Main
# -----------------------------
api_key = os.environ["DATA_GOV_API_KEY"]
limit_value = 10
air_quality_data = get_air_quality_data(api_key, limit_value)
