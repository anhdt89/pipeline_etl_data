# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 17:32:41 2022

@author: Admin
"""

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import airflow

# source
def read_file_excel(path, file_name, sheet_name):
    dataframe = pd.read_excel(path + file_name, sheet_name=sheet_name) #corrected argument name
    return dataframe

# connection
def connect_to_gcs(credentials, project_id, conn_type):
    credentials = service_account.Credentials.from_service_account_file(credentials)
    if conn_type == 'bigquery':
        client = bigquery.Client(credentials=credentials, project=project_id)
    elif conn_type == 'storage':
        client = storage.Client(credentials=credentials, project=project_id)
    print('connection success!!!')
    return client

# import to gcs
def load_data_to_gcs(data, credentials, project_id, bucket_name, path, file_name, ext):
    client = connect_to_gcs(credentials, project_id, 'storage')
    bucket = client.get_bucket(bucket_name)
    bucket.blob('{path}/{file_name}.{ext}'.format(path=path, file_name=file_name, ext=ext)).upload_from_string(data.to_csv(index=False, encoding='utf-8'), '{ext}'.format(ext=ext))

# import to bigquery
def load_data_to_bigquery(credentials, project_id, table_id, uri, file_type):
    client = connect_to_gcs(credentials, project_id, 'bigquery')
    job_config = bigquery.LoadJobConfig(autodetect=True, skip_leading_rows=1, source_format=bigquery.SourceFormat.CSV,)
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def call_procedure_test(credentials, project_id, procedure_name):
    credentials = service_account.Credentials.from_service_account_file(credentials)
    client = bigquery.Client(credentials=credentials, project=project_id)
    job = client.query(procedure_name)
    
    



# path server
project_path = '/root/airflow/dags'

# data local
path = '/root/airflow/dags/pipeline_etl_data/'
file_name = 'Daily quality report of 4G VTC network on 01162022 PV.xlsx'
sheet_name = '8 day Peak Overview'

# service account, project id
# gcs_path_service_account = 'D:/project/pipeline_etl_data/service_account_key.json'
gcs_path_service_account = '/root/airflow/dags/pipeline_etl_data/service_account_key.json'
gcs_project_id = 'fifth-sunup-338412'
dataset = 'dataset_test'

# service type
gcs_conn_type = 'storage'
bq_conn_type = 'bigquery'

# path gcs
gcs_path = 'KPItest'
gcs_file_name = 'test'
gcs_bucket_name = 'anhdt2110'

uri_file_name = 'gs://anhdt2110/KPItest/test.csv'

# bigquery
table_id = '{project_id}.{dataset}.{table_name}'
# table_id.format(project_id = gcs_project_id, dataset = dataset, table_name = 'test_auto')


daily_dags = {
    'owner': 'longnv42',
    # 'retries': 1,
    # 'retry_delay': datetime.timedelta(minutes=1),
    'start_date': airflow.utils.dates.days_ago(1),
    'schedule_interval': '@once',
    # 'on_failure_callback': on_failure_callback,
}


dag = DAG(dag_id="pipeline_import_auto_1", default_args=daily_dags,)
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

call_procedure_test = PythonOperator(task_id='call_procedure_test',
                                     python_callable = call_procedure_test,
                                     op_kwargs={
                                          "credentials": gcs_path_service_account,
                                          "project_id": gcs_project_id,
                                          "procedure_name": '''call {procedure_name}('{p_zone}', '{p_time}')'''.format(procedure_name = 'fifth-sunup-338412.dataset_test.insert_data_from_manual_to_auto', p_zone='KAN', p_time='2022-01-14'),
                                          },
                                     dag=dag,
                                     )

start_job >> load_data_to_bigquery >> call_procedure_test >> end_job

