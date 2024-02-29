from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args={
    'owner':'airflow',
    'start_date':datetime(2024, 2, 27),
    'depends_on_past':False,
    'email': ['omsaiyatthapu@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5), 
}

dag=DAG('fetch_cric_stats',
        default_args=default_args,
        description='Runs an external python script',
        schedule_interval='@daily',
        catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='python /home/airflow/gcs/dags/scripts/extract_data_and_upload_to_gcs.py'
    )
