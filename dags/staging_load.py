import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from configparser import ConfigParser
from datetime import datetime, timedelta

config_path=r'/opt/airflow/config/'
raw_files_path=r'/opt/airflow/raw_files/'


def config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    
    return db

def get_engine(schema='public'):
    dbschema=schema
    param=config(filename=config_path+r'database.ini', section='postgresql')
    db_uri='postgresql+psycopg2://'+param['user']+':'+param['password']+'@'+param['host']+'/'+param['database']
    engine=create_engine(db_uri,connect_args={'options': '-csearch_path={}'.format(dbschema)})
    return engine

def cast_cols(df):
    groups=df.columns.groupby(df.dtypes)
    try :
        for i in {k.name: v for k, v in groups.items()}['object']:
            if i in ['START','STOP','BIRTHDATE','DEATHDATE']:
                df[i]=pd.to_datetime(df[i])
            else :
                df[i]=df[i].astype('string')
    except :
        pass


##################################
#---------DAG SCHEDULING---------#
##################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task

with DAG(
    'staging_layer_load',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['test@test.com'],
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='load all the files in staging layer tables.',
    #schedule_interval='@once',
    schedule_interval=None,
    start_date=datetime(2024, 7, 1),
    catchup=False,
    tags=['staging data load'],
    params={'exec_mode':'replace'} # can be replace (Truncate & load) or append (Load in existing table)
) as dag:

    
    files=['encounters.csv','organizations.csv','patients.csv','payers.csv','procedures.csv']

    @task(provide_context=True)
    def load_files(filename, **kwargs):
            df=pd.read_csv(f'/opt/airflow/raw_files/{filename}')
            cast_cols(df)   # casting columns to required datatypes
            df=df.rename(str.lower,axis='columns') # renaming columns to lower case easy to access in postgres
            df.to_sql(f'{filename.split(".")[0]}',con=get_engine('staging'),index=False,if_exists=kwargs['params']['exec_mode'])  
    

    load_files.expand(filename=files)

    
    
