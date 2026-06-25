import requests
from include.etl_control import EtlControl
from datetime import datetime
import io
import boto3
from include.logging import setup_logger, get_logger


setup_logger()
logger = get_logger()


def get_available_months(year, taxi_type, etl_control, base_url):
    available_months = []
    already_loaded = set(etl_control.get_loaded_months())

    for month in range(1, 13):
        month_str = f"{month:02d}"
        year_month = f"{year}-{month_str}"

        if year_month in already_loaded:
            continue

        dataset_url = f"{base_url}/{taxi_type}_tripdata_{year_month}.parquet"

        try:
            response = requests.head(dataset_url,timeout=10)

            if response.status_code == 200:
                available_months.append(year_month)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Error checking {year_month}: {e}")
    
    if not available_months:
        logger.info("There are no available months to extract")
    else:
        logger.info(f"Found {len(available_months)} available months to extract: {available_months}")
    
    return available_months


def _handle_failure(etl_control, year_month, error_message, counter):
    etl_control.mark_failed(year_month=year_month, error_message=error_message)
    logger.warning(f"There was an error processing {year_month}: {error_message}")
    counter += 1

    if counter > 3:
        logger.error("Too many failures, stopping pipeline")
        raise RuntimeError(f"Stopped extraction after {counter} failed months")

    return counter


def extract_and_load_to_s3(available_months, taxi_type, year, etl_control, BUCKET_NAME, BASE_URL):

    counter = 0

    s3_client = boto3.client("s3")

    months_need_to_download = etl_control.get_months_needing_download(available_months)

    if not months_need_to_download:
        return logger.info("There are no months to download")
    
    
    for year_month in months_need_to_download:
        dataset_url = f"{BASE_URL}/{taxi_type}_tripdata_{year_month}.parquet"
        file_name = f"{taxi_type}_tripdata_{year_month}.parquet"
        s3_key = f"raw/{taxi_type}/{year}/{file_name}"

        try:
            get_response = requests.get(dataset_url, stream=True, timeout=30)
        
            if get_response.status_code != 200:
                counter = _handle_failure(
                    etl_control, year_month,
                    f"HTTP {get_response.status_code} when fetching {dataset_url}",
                    counter
                )
                continue

            file_buffer = io.BytesIO()
            for chunk in get_response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file_buffer.write(chunk)
            
            file_buffer.seek(0)

            s3_client.upload_fileobj(file_buffer, Bucket=BUCKET_NAME, Key=s3_key)
            etl_control.mark_downloaded(year_month=year_month, s3_raw_path=s3_key)

            logger.info(f"The {year_month} successfully extract to s3")
                
        except Exception as e:
            counter = _handle_failure(
                etl_control, year_month, str(e), counter
            )
            
    
