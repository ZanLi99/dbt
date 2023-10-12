import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#########################################################
#
#   Load Environment Variables
#
#########################################################

API_KEY= Variable.get("api_key") 


########################################################
#
#   DAG Settings
#
#########################################################



dag_default_args = {
    'owner': 'BDE_LAB_6',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='connect_test',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

# Cities coordinates
sydney_lat = -33.865143
sydney_lon = 151.209900
brisbane_lat = -27.470125
brisbane_lon = 153.021072
melbourne_lon = 144.946457
melbourne_lat = -37.840935

AIRFLOW_DATA = "/home/airflow/gcs/dags"
NSW_LGA = AIRFLOW_DATA+"/NSW_LGA/"
Census_LGA = AIRFLOW_DATA+"/Census_LGA/"

def import_date_func_lga_code(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    df_code = pd.read_csv(NSW_LGA+'NSW_LGA_CODE.csv')
    return df_code

def import_date_func_lga_suburb(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    df_code = pd.read_csv(NSW_LGA+'NSW_LGA_SUBURB.csv')
    return df_code


def insert_data_func_lge_code(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df = ti.xcom_pull(task_ids=f'import_date_func_lga_code')
    if len(insert_df) > 0:
        col_names = ['LGA_CODE','LGA_NAME']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_code (lga_code, lga_name)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None    

def insert_data_func_suburb_code(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df = ti.xcom_pull(task_ids=f'import_date_func_lga_suburb')
    if len(insert_df) > 0:
        col_names = ['LGA_NAME', 'SUBURB_NAME']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_suburb (lga_name, suburb_name)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None 

#########################################################
#
#   DAG Operator Setup
#
#########################################################

# Import LGA_CODE csv
import_data_lga_code_task = PythonOperator(
    task_id='import_date_func_lga_code',
    python_callable=import_date_func_lga_code,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_psql_table_nsw_lga_code_task = PostgresOperator(
    task_id="create_psql_table_nsw_lga_code_task",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS raw.nsw_lga_code (
            lga_code        INT,
            lga_name        VARCHAR
            );
    """,
    dag=dag
)

insert_data_code_task = PythonOperator(
    task_id="insert_data_func_lge_code",
    python_callable=insert_data_func_lge_code,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Import LGA_SUBURB csv
import_data_lga_suburb_task = PythonOperator(
    task_id='import_date_func_lga_suburb',
    python_callable=import_date_func_lga_suburb,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_psql_table_nsw_lga_suburb_task = PostgresOperator(
    task_id="create_psql_table_nsw_lga_suburb_task",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS raw.nsw_lga_suburb (
            lga_name        VARCHAR,
            suburb_name     VARCHAR
            );
    """,
    dag=dag
)

insert_data_suburb_task = PythonOperator(
    task_id="insert_data_func_suburb_code",
    python_callable=insert_data_func_suburb_code,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_psql_table_nsw_lga_code_task >> import_data_lga_code_task >> insert_data_code_task
create_psql_table_nsw_lga_suburb_task >> import_data_lga_suburb_task >> insert_data_suburb_task