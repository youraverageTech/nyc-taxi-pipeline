from source.etl_control import EtlControl
import snowflake.connector


def load_to_snowflake(connection_provider, stage_name="nyc_taxi_stage", target_table="staging.raw_nyc_taxi_tripdata"):
    etl_control = EtlControl(connection_provider)

    months_to_load = etl_control.get_months_needing_load()

    for year_month, s3_raw_path in months_to_load:
        try:
            with connection_provider() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        COPY INTO {target_table}
                        FROM @{stage_name}/{s3_raw_path}
                        FILE_FORMAT (TYPE = PARQUET)
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    """)

            etl_control.mark_loaded(year_month)
        
        except Exception as e:
            etl_control.mark_failed(year_month=year_month, error_message=str(e))