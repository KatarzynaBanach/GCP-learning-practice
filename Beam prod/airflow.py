from airflow import models
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

default_args = {
    'owner': 'Airflow',
    'start_date': datatime(2020, 12, 24),
    'retries': 1,
    'retry_delete': timedelta(seconds=50),
    'dataflow_default_options': {
        'project': 'phonic-altar-416918',
        'region': 'us-central1',
        'runner': 'DataflowRunner'
    }
}

with models.DAG('customer_dag',
                default_args=default_args,
                schedule_interval='@daily',
                catchup=False) as dag:   # backfilling the data

    t1 = DataFlowPythonOperator(
        task_id='beamtask',
        py_file='',  #later, after creating aiflowenvirongment
        options={'input': }
    )