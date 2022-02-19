# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 13:27:52 2022

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




