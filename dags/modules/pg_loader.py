import logging
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PgLoader:
    def __init__(self, postgres_conn_id, schema, table_name, date1, date2, local_dwnld_base, source, request_id):
        self.pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.schema = schema
        self.table_name = table_name
        self.date1 = date1
        self.date2 = date2
        self.local_dwnld_base = local_dwnld_base
        self.source = source
        self.request_id = request_id
        logging.info(f'PgLoader initialized with schema: {schema}, table_name {table_name}, date1: {date1} and date2: {date2}')

    def load_data(self):
        conn = self.pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            conn.autocommit = False
            
            delete_query = f"DELETE FROM {self.schema}.{self.table_name} WHERE date_time BETWEEN '{self.date1}' AND '{self.date2}'"
            cursor.execute(delete_query)
            deleted_count = cursor.rowcount
            logging.info(f"Deleted {deleted_count} rows from {self.schema}.{self.table_name} where date_time BETWEEN '{self.date1}' and '{self.date2}'.")

            log_files_dir = os.path.join(self.local_dwnld_base, self.source, str(self.request_id))

            for filename in os.listdir(log_files_dir):
                if filename.endswith('.csv'):
                    file_path = os.path.join(log_files_dir, filename)
                    logging.info(f"Loading data from {file_path} into {self.schema}.{self.table_name}.")

                    with open(file_path, 'r') as f:
                        cursor.copy_expert(f"COPY {self.schema}.{self.table_name} FROM stdin WITH CSV HEADER", f)
            
            conn.commit()
            logging.info("Data loading completed successfully.")

        except Exception as e:
            logging.error(f"Error occurred: {e}")
            conn.rollback()  # Rollback if any error occurs
            logging.info("Transaction rolled back.")

        finally:
            cursor.close()
            conn.close()
            
