from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime, timedelta
import logging
import os
import shutil

from modules.logs_api import LogsApi
from modules.pg_loader import PgLoader

########################################################################################################################
#                                       CONSTANTS 

LOCAL_DWNLD_BASE = '/tmp/metrica/logs_api/'
SOURCE = LogsApi.SOURCE[1]
COUNTER_ID = Variable.get("ekfgroup")

#######################################################################################################################

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='metrica_visits_to_pg',
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1, 
    params={
        'date_start': None, 
        'date_end': None
    }
) as dag:
    
    @task
    def create_log_request(**kwargs):
        dag_run = kwargs['dag_run']
        date1 = dag_run.conf.get('date_start')
        date2 = dag_run.conf.get('date_end')

        if date1 is None or date2 is None:
            date1 = (kwargs['data_interval_end'] - timedelta(days=3)).strftime('%Y-%m-%d')
            date2 = (kwargs['data_interval_end'] - timedelta(days=1)).strftime('%Y-%m-%d')

        request_id = LogsApi.create_log_request(date1, date2, SOURCE)
        return request_id
    
    @task
    def get_log_request_status(request_id):
        status = LogsApi.get_log_request_status(request_id)
        return status
    
    @task
    def download_log_files(request_id, status):
        if status:
            LogsApi.download_log_files(request_id, status, SOURCE, LOCAL_DWNLD_BASE)
        return request_id
    
    @task
    def logs_to_postgres(request_id, status):
        logging.info(status)
        pg_loader = PgLoader(
            postgres_conn_id='pg_ekf',
            schema='metrica_logs_api',
            table_name=f'{SOURCE}_{COUNTER_ID}',
            date1=status["date1"],
            date2=status["date2"],
            local_dwnld_base=LOCAL_DWNLD_BASE,
            source=SOURCE,
            request_id=request_id
        )

        pg_loader.load_data()
        return request_id

    @task(trigger_rule='all_done')
    def clean_log_files(request_id):
        LogsApi.clean_log_files(request_id)
        return request_id

    @task(trigger_rule='all_done')
    def clean_dwnld_folder(request_id):
        folder_path = os.path.join(LOCAL_DWNLD_BASE, SOURCE, str(request_id))
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            logging.info(f"Folder {folder_path} has been cleaned up.")
        else:
            logging.info(f"Folder {folder_path} does not exist, no need to clean.")


########################################################################################################################
#  
    request_id = create_log_request(params=dag.params)

    status = get_log_request_status(request_id)

    request_id = download_log_files(request_id, status)

    request_id = logs_to_postgres(request_id, status)

    request_id = clean_log_files(request_id)
    
    clean_dwnld_folder(request_id)
    
#######################################################################################################################

