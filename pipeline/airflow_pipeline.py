# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 14:56:32 2022

@author: Admin
"""


import sys, os, io
from dotenv import load_dotenv
load_dotenv(sys.path.append(os.getenv('/root/airflow/dags/')))
sys.path.append(os.getenv('/root/airflow/dags/'))

# from packages.import_packages import *
import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from common.functions import *
from pipeline.init_params import *


daily_dags = {
    'owner': 'longnv42',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': airflow.utils.dates.days_ago(1),
    'schedule_interval': '@once',
    # 'on_failure_callback': on_failure_callback,
}

dag = DAG(dag_id="pipeline_import_auto", default_args=daily_dags,)
start_job = BashOperator(task_id='start_job', bash_command='echo "starting job"', dag=dag)
end_job = BashOperator(task_id='end_job', bash_command='echo "ending job"', dag=dag)
load_data_to_bigquery = PythonOperator(task_id ='load_data_to_bigquery',
                                       python_callable = load_data_to_bigquery,
                                       op_kwargs={
                                            "credentials": gcs_path_service_account,
                                            "project_id": gcs_project_id,
                                            "table_id": table_id.format(project_id=gcs_project_id, dataset=dataset, table_name='test_auto'),
                                            "uri": uri_file_name,
                                            "file_type": 'csv',
                                            },
                                       dag=dag,
                                       )
start_job >> load_data_to_bigquery >> end_job


