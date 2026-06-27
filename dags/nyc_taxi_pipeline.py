from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
import os
from include.logging import setup_logger, get_logger
from include.etl_control import EtlControl

from pathlib import Path
import time


load_dotenv()
setup_logger()
logger = get_logger()
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BUCKET_NAME = os.getenv("BUCKET_NAME")
TAXI_TYPE = "yellow"
HTML_FILE = (Path(__file__).resolve().parent.parent / "include" / "email_content.html")
html_contents = HTML_FILE.read_text(encoding="utf-8")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")


def etl_control():
        hook = SnowflakeHook(snowflake_conn_id="nyc_taxi_pipeline")

        return EtlControl(hook.get_conn)


# create logs for dag failure
def failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id

    logger.error(f"Failure happened in DAG {dag_id}, task {task_id}")


# adding retries and alert email notification if the pipeline failed
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": [failure_callback,
        send_smtp_notification(
            smtp_conn_id="alert_email",
            from_email=SENDER_EMAIL,
            to=RECEIVER_EMAIL,
            subject="🚨 Airflow Failur Alert | DAG {{ dag.dag_id }} | Task {{ ti.task_id }} Failed",
            html_content=html_contents,
    )]
}

@dag(
    dag_id = "nyc_taxi_pipeline",
    start_date = datetime(2026, 6, 1),
    schedule = "@monthly", 
    catchup = False,
    default_args = default_args
)
def nyc_taxi_pipeline():

    # checking the available months in source website.
    @task
    def check_available_months(**context):
        from include.extract import get_available_months

        logger.info("Starting checking available months...")
        start_date = time.time()
        ds = context['ds']
        year = int(ds.split("-")[0])

        available_month = get_available_months(year=year, taxi_type=TAXI_TYPE, 
                                               etl_control=etl_control(), base_url=BASE_URL)
        
        end_date = time.time()
        logger.info(f"Finishing checking available months. Execution time: {(end_date - start_date)}")

        return available_month
    

    # checking if there are months that available or not, if no skip the extract and loading process.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def has_new_data(available_month):
        return len(available_month) > 0


    # extracting data from website to S3 as raw landing zone
    @task
    def extract_to_s3(available_month, **context):
        from include.extract import extract_and_load_to_s3

        logger.info("Starting extract to S3...")
        start_date = time.time()
        ds = context['ds']
        year = int(ds.split("-")[0])

        extract_and_load_to_s3(available_months=available_month, 
                               taxi_type=TAXI_TYPE, 
                               year=year, 
                               etl_control=etl_control(), 
                               BUCKET_NAME=BUCKET_NAME, 
                               BASE_URL=BASE_URL)
        
        end_date = time.time()
        logger.info(f"Finishing extract to S3. Execution time: {(end_date - start_date)}")


    # loading the extracted data into snowflake raw table
    @task
    def load_to_snowflakes():
        from include.loading import load_to_snowflake

        logger.info("Starting load to Snowflake...")
        start_date = time.time()

        load_to_snowflake(etl_control=etl_control())

        end_date = time.time()
        logger.info(f"Finishing load to Snowflake. Execution time: {(end_date - start_date)}")


    # transform the data using dbt 
    @task_group(group_id="dbt_transform")
    def dbt_transform():

        @task.bash(trigger_rule=TriggerRule.NONE_FAILED)
        def dbt_deps(): # run dbt deps for dependency dbt_utils
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt deps"
         
        @task.bash(trigger_rule=TriggerRule.NONE_FAILED)
        def dbt_seed(): # run dbt seed for zone lookup data
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt seed"
         
        @task.bash(trigger_rule=TriggerRule.NONE_FAILED)
        def dbt_run(): # run dbt run
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt run"
         
        @task.bash(trigger_rule=TriggerRule.NONE_FAILED)
        def dbt_test(): # run dbt test
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt test"

        
        dbt_deps() >> dbt_seed() >> dbt_run() >> dbt_test()

    
    available_month = check_available_months()
    gate = has_new_data(available_month)
    extracted = extract_to_s3(available_month)
    loaded = load_to_snowflakes()
    
    gate >> extracted >> loaded >> dbt_transform()


nyc_taxi_pipeline()