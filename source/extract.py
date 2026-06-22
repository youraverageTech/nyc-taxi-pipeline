import requests
from source.etl_control import EtlControl
from datetime import datetime
import io
import boto3

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
params = {}
BUCKET_NAME = "nyc-yellow-taxi-data-317871535186-ap-southeast-1-an"
taxi_type = "yellow"
year = datetime.now().year

def get_available_months(year, taxi_type):
    available_months = []
    already_loaded = set(EtlControl(params).get_loaded_months())

    for month in range(1, 13):
        month_str = f"{month:02d}"
        year_month = f"{year}-{month_str}"

        if year_month in already_loaded:
            continue

        dataset_url = f"{BASE_URL}/{taxi_type}_tripdata_{year_month}.parquet"

        try:
            response = requests.head(dataset_url,timeout=10)

            if response.status_code == 200:
                available_months.append(year_month)
        except Exception as e:
            print(f"Error checking {year_month}: {e}")
    
    return available_months

def extract_and_load_to_s3(available_months, taxi_type, year):
    etl_control = EtlControl(params)
    s3_client = boto3.client("s3")

    months_need_to_download = etl_control.get_months_needing_download(available_months)
    
    for year_month in months_need_to_download:
        dataset_url = f"{BASE_URL}/{taxi_type}_tripdata_{year_month}.parquet"
        file_name = f"{taxi_type}_tripdata_{year_month}.parquet"
        s3_key = f"raw/{taxi_type}/{year}/{file_name}"

        try:
            get_response = requests.get(dataset_url, stream=True, timeout=30)
        
            if get_response.status_code != 200:
                etl_control.mark_failed(
                    year_month=year_month,
                    error_message=f"HTTP {get_response.status_code} when fetching {dataset_url}",
                )
                continue

            file_buffer = io.BytesIO()
            for chunk in get_response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file_buffer.write(chunk)
            
            file_buffer.seek(0)

            s3_client.upload_fileobj(file_buffer, Bucket=BUCKET_NAME, Key=s3_key)
            etl_control.mark_downloaded(year_month=year_month, s3_raw_path=s3_key)
                
        except Exception as e:
            etl_control.mark_failed(year_month=year_month, error_message=str(e))
    

if __name__ == "__main__":
    months = get_available_months(year, taxi_type)
    print(months)
