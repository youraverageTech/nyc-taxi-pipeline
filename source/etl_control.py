from datetime import datetime
import snowflake.connector


class EtlControl:
    def __init__(self, connection_params: dict):
        self.connection_params = connection_params


    def _get_connection(self):
        return snowflake.connector.connect(**self.connection_params)
    
    
    def get_pending_months(self, expected_month: list):
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT year_month
                    FROM staging.etl_control
                    WHERE status = 'loaded'
                """)

                loaded = {row[0] for row in cur.fetchall()}
        
        return [month for month in expected_month if month not in loaded]
    

    def _upsert(self, year_month: str, **fields):
        all_fields = {"year_month": year_month, **fields, "updated_at": datetime.now()}

        columns = list(all_fields.keys())
        values = list(all_fields.values())

        columns_str = ", ".join(columns)
        values_placeholder = ", ".join(["%s"] * len(values))
        update_str = ", ".join([f"target.{col} = source.{col}" for col in columns if col != "year_month"])
        insert_values = ", ".join([f"source.{col}" for col in columns])

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                MERGE INTO staging.etl_control AS target
                USING (VALUES ({values_placeholder})) AS source ({columns_str})
                ON target.year_month = source.year_month
                WHEN MATCHED THEN
                    UPDATE SET {update_str}
                WHEN NOT MATCHED THEN
                    INSERT ({columns_str}) VALUES ({insert_values})
            """, values)


    def mark_pending(self, year_month, source_url):
        return self._upsert(year_month, status='pending', source_url=source_url)


    def mark_loaded(self, year_month):
        return self._upsert(year_month, status="loaded", loaded_at=datetime.now())

    
    def mark_downloaded(self, year_month, s3_raw_path):
        return self._upsert(year_month, status="downloaded", s3_raw_path=s3_raw_path, downloaded_at=datetime.now())


    def mark_failed(self, year_month, error_message):
        return self._upsert(year_month, status="failed", error_message=error_message)