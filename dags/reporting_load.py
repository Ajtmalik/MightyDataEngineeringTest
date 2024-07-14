#python functions

import pandas as pd

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
    'curated_layer_load',
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
    tags=['curated data load']
) as dag:

    load_dim_patient= PostgresOperator(
        task_id="load_dim_patient",
        postgres_conn_id="postgres_main",
        sql="sql/dim_patient.sql",
    )
    
    load_dim_payer= PostgresOperator(
        task_id="load_dim_payer",
        postgres_conn_id="postgres_main",
        sql="sql/dim_payer.sql",
    )
    
    load_dim_organization= PostgresOperator(
        task_id="load_dim_organization",
        postgres_conn_id="postgres_main",
        sql="sql/dim_organization.sql",
    )

    load_dim_reason= PostgresOperator(
        task_id="load_dim_reason",
        postgres_conn_id="postgres_main",
        sql="sql/dim_reason.sql",
    )
    
    load_dim_treatment= PostgresOperator(
        task_id="load_dim_treatment",
        postgres_conn_id="postgres_main",
        sql="sql/dim_treatment.sql",
    )
    
    load_fact_procedures= PostgresOperator(
        task_id="load_fact_procedures",
        postgres_conn_id="postgres_main",
        sql="sql/fact_procedures.sql",
    )
    
    load_fact_encounters= PostgresOperator(
        task_id="load_fact_encounters",
        postgres_conn_id="postgres_main",
        sql="sql/fact_encounters.sql",
    )
    

    load_dim_patient >> [load_fact_procedures, load_fact_encounters]
    load_dim_payer >> [load_fact_procedures, load_fact_encounters]
    load_dim_organization >> [load_fact_procedures, load_fact_encounters]
    load_dim_reason >> [load_fact_procedures, load_fact_encounters]
    load_dim_treatment >> [load_fact_procedures, load_fact_encounters]

    
    
