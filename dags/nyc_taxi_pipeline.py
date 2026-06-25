from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
import os
from include.logging import setup_logger, get_logger


load_dotenv()
setup_logger()
logger = get_logger()
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BUCKET_NAME = os.getenv("BUCKET_NAME")
TAXI_TYPE = "yellow"

def get_snowflake_connection():
        hook = SnowflakeHook(snowflake_conn_id="nyc_taxi_pipeline")

        return hook.get_conn()

@dag(
    dag_id = "nyc_taxi_pipeline",
    start_date = datetime(2026, 6, 1),
    schedule = "@monthly",
    catchup = False
)
def nyc_taxi_pipeline():
    
    @task
    def check_available_months(**context):
        from include.extract import get_available_months

        logger.info("Starting checking available months...")
        start_date = datetime.now().hour
        ds = context['ds']
        year = int(ds.split("-")[0])

        available_month = get_available_months(year=year, taxi_type=TAXI_TYPE, 
                                               connection_provider=get_snowflake_connection, BASE_URL=BASE_URL)
        
        end_date = datetime.now().hour
        logger.info(f"Finishing checking available months. Execution time: {(end_date - start_date)}")

        return available_month
    

    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def has_new_data(available_month):
        return len(available_month) > 0


    @task
    def extract_to_s3(available_month, **context):
        from include.extract import extract_and_load_to_s3

        logger.info("Starting extract to S3...")
        start_date = datetime.now().hour
        ds = context['ds']
        year = int(ds.split("-")[0])

        extract_and_load_to_s3(available_months=available_month, 
                               taxi_type=TAXI_TYPE, 
                               year=year, 
                               connection_provider=get_snowflake_connection, 
                               BUCKET_NAME=BUCKET_NAME, 
                               BASE_URL=BASE_URL)
        
        end_date = datetime.now().hour
        logger.info(f"Finishing extract to S3. Execution time: {(end_date - start_date)}")

    @task
    def load_to_snowflake():
        from include.loading import load_to_snowflake

        logger.info("Starting load to Snowflake...")
        start_date = datetime.now().hour

        load_to_snowflake(connection_provider=get_snowflake_connection)

        end_date = datetime.now().hour
        logger.info(f"Finishing load to Snowflake. Execution time: {(end_date - start_date)}")


    @task_group(group_id="dbt_transform")
    def dbt_transform():

        logger.info("Starting transform dbt...")
        start_date = datetime.now().hour

        @task.bash(trigger_rule=TriggerRule.ALL_DONE)
        def dbt_deps():
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt deps"
         
        @task.bash(trigger_rule=TriggerRule.ALL_DONE)
        def dbt_seed():
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt seed"
         
        @task.bash(trigger_rule=TriggerRule.ALL_DONE)
        def dbt_run():
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt run"
         
        @task.bash(trigger_rule=TriggerRule.ALL_DONE)
        def dbt_test():
            return "cd /usr/local/airflow/dbt/nyc_taxi_dbt && dbt test"
        
        end_date = datetime.now().hour
        logger.info(f"Finishing transform dbt. Execution time: {(end_date - start_date)}")
        
        dbt_deps() >> dbt_seed() >> dbt_run() >> dbt_test()

    
    available_month = check_available_months()
    gate = has_new_data(available_month)
    extracted = extract_to_s3(available_month)
    loaded = load_to_snowflake()
    

    gate >> extracted >> loaded >> dbt_transform()

nyc_taxi_pipeline()