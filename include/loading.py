from include.etl_control import EtlControl
from datetime import datetime
from zoneinfo import ZoneInfo

def load_to_snowflake(connection_provider, stage_name="nyc_taxi_stage", target_table="staging.raw_nyc_taxi_tripdata"):
    etl_control = EtlControl(connection_provider)

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
            with connection_provider() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    print("Part 1 loading data success")
                    cur.execute(update_query, (year_month, datetime.now(tz=ZoneInfo("Asia/Jakarta"))))
                    print(f"Loading {year_month} data success")
            etl_control.mark_loaded(year_month)
        except Exception as e:
            etl_control.mark_failed(year_month=year_month, error_message=str(e))