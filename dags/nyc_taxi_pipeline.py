from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
import os
import subprocess

load_dotenv()

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BUCKET_NAME = os.getenv("BUCKET_NAME")
TAXI_TYPE = "yellow"

def get_snowflake_connection():
        hook = SnowflakeHook(snowflake_conn_id="nyc_taxi_pipeline")

        return hook.get_conn()

def run_dbt_command(command: list[str]):
     subprocess.run(
          command,
          check=True,
          cwd=""
     )

@dag(
    dag_id = "nyc_taxi_pipeline",
    start_date = datetime(2026, 6, 1),
    schedule = "@monthly",
    catchup = False
)
def nyc_taxi_pipeline():
    
    @task
    def check_available_months():
        from include.extract import get_available_months

        year = datetime.now().year

        available_month = get_available_months(year=year, taxi_type=TAXI_TYPE, 
                                               connection_provider=get_snowflake_connection, BASE_URL=BASE_URL)

        return available_month
    
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def has_new_data(available_month):
        return len(available_month) > 0
    
    @task
    def extract_to_s3(available_month):
        from include.extract import extract_and_load_to_s3

        year = datetime.now().year

        extract_and_load_to_s3(available_months=available_month, 
                               taxi_type=TAXI_TYPE, 
                               year=year, 
                               connection_provider=get_snowflake_connection, 
                               BUCKET_NAME=BUCKET_NAME, 
                               BASE_URL=BASE_URL)

    @task
    def load_to_s3():
        from include.loading import load_to_snowflake

        load_to_snowflake(connection_provider=get_snowflake_connection)

    @task_group(group_id="running_dbt")
    def running_dbt():
         
        @task.bash(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        def dbt_seed():
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt seed"
         
        @task.bash
        def dbt_run():
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt run"
         
        @task.bash
        def dbt_test():
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt test"
        
        dbt_seed() >> dbt_run() >> dbt_test()

    
    available_month = check_available_months()
    gate = has_new_data(available_month)
    extracted = extract_to_s3(available_month)
    loaded = load_to_s3()
    

    gate >> extracted >> loaded >> running_dbt()

nyc_taxi_pipeline()