from include.etl_control import EtlControl
from datetime import datetime
from zoneinfo import ZoneInfo
from include.logging import setup_logger, get_logger
from include.extract import _handle_failure


setup_logger()
logger = get_logger()


def load_to_snowflake(etl_control, stage_name="nyc_taxi_stage", target_table="staging.raw_nyc_taxi_tripdata"):
    counter = 0
    months_to_load = etl_control.get_months_needing_load()

    for year_month, s3_raw_path in months_to_load:
        query = f"""
            COPY INTO {target_table}
            FROM @{stage_name}/{s3_raw_path}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """

        update_query = f"""
            UPDATE {target_table}
            SET year_month = %s, loaded_at = %s
            WHERE year_month IS NULL
        """

        try:
            with etl_control._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    cur.execute(update_query, (year_month, datetime.now(tz=ZoneInfo("Asia/Jakarta"))))

                    logger.info(f"Loading the {year_month} to raw table success")

            etl_control.mark_loaded(year_month)

        except Exception as e:
            counter = _handle_failure(
                etl_control, year_month, str(e), counter
            )