# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 14:49:21 2022

@author: Admin
"""

# data local
path = 'C:/Users/Admin/Downloads/Test GCP/'
file_name = 'Daily quality report of 4G VTC network on 01162022 PV.xlsx'
sheet_name = '8 day Peak Overview'

# service account, project id
gcs_path_service_account = 'D:/project/pipeline_etl_data/service_account_key.json'
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

