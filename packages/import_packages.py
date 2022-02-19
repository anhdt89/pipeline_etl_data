# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 13:26:26 2022

@author: Admin
"""

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime