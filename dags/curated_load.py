#python functions

import pandas as pd
import gzip
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from configparser import ConfigParser
import gc
import sys
import time
from datetime import datetime, timedelta 


##################################
#---------DAG SCHEDULING---------#
##################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    'reporting_layer_load',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['test@test.com'],
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='load all fact and dim tables in curated layer from staging schema.',
    #schedule_interval='@once',
    schedule_interval=None,
    start_date=datetime(2024, 7, 1),
    catchup=False,
    tags=['reporting data load']
) as dag:

    load_united= PostgresOperator(
        task_id="load_united",
        postgres_conn_id="postgres_main",
        sql="sql/facts_united.sql",
    )
    
    load_humana= PostgresOperator(
        task_id="load_humana",
        postgres_conn_id="postgres_main",
        sql="sql/facts_humana.sql",
    )
    
    load_united
    load_humana
    

    
    
