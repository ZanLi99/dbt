import os
import logging
import requests
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    dag_id='ipmort_csv',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/dags"
NSW_LGA = AIRFLOW_DATA+"/NSW_LGA/"
Census_LGA = AIRFLOW_DATA+"/Census_LGA/"
# conn_params = {
#     "database": "postgres",
#     "user": "postgres",
#     "password": "1122",
#     "host": "35.189.29.93",
#     "port": "5432"
# }
#FACTS = AIRFLOW_DATA+"/facts/"


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

def import_load_dim_code_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df_code = pd.read_csv(NSW_LGA+'NSW_LGA_CODE.csv')

    if len(df_code) > 0:
        col_names = ['LGA_CODE','LGA_NAME']

        values = df_code[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO lga_code(LGA_CODE,LGA_NAME)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df_code))
        conn_ps.commit()
    else:
        None

    return None


def import_load_dim_suburb_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(NSW_LGA+'NSW_LGA_SUBURB.csv')

    if len(df) > 0:
        col_names = ['LGA_NAME','SUBURB_NAME']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO lga_suburb(LGA_NAME,SUBURB_NAME)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None


# def import_load_dim_brand_func(**kwargs):

#     #set up pg connection
#     ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
#     conn_ps = ps_pg_hook.get_conn()


#     #generate dataframe by combining files
#     df = pd.read_csv(DIMENSIONS+'brand.csv')

#     if len(df) > 0:
#         col_names = ['id','brand']

#         values = df[col_names].to_dict('split')
#         values = values['data']
#         logging.info(values)

#         insert_sql = """
#                     INSERT INTO raw.brand(id,brand)
#                     VALUES %s
#                     """

#         result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
#         conn_ps.commit()
#     else:
#         None

#    return None

   
# def import_csv_to_postgres(csv_file, table_name, conn_params):
#     # Read the CSV file into a DataFrame
#     df = pd.read_csv(csv_file)
    
#     # Establish a connection to the PostgreSQL database
#     conn = psycopg2.connect(**conn_params)
#     cursor = conn.cursor()
    
#     # Create a dynamic table based on the columns in the CSV
#     create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY"
#     for col in df.columns[1:]:
#         create_table_query += f", {col} INT"
#     create_table_query += ")"
    
#     cursor.execute(create_table_query)
#     conn.commit()
    
#     # Insert data into the dynamic table
#     insert_data_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s' for _ in df.columns])})"
    
#     for row in df.itertuples(index=False, name=None):
#         cursor.execute(insert_data_query, row)
    
#     conn.commit()
    
#     # Close the database connection
#     cursor.close()
#     conn.close()

# def import_load_facts_func(**kwargs):

#     #set up pg connection
#     ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
#     conn_ps = ps_pg_hook.get_conn()

#     #get all files with filename including the string '.csv'
#     filelist = [k for k in os.listdir(FACTS) if '.csv' in k]

#     #generate dataframe by combining files
#     df = pd.concat([pd.read_csv(os.path.join(FACTS, fname)) for fname in filelist], ignore_index=True)

#     if len(df) > 0:
#         col_names = ['date','order_id','category_id','subcategory_id','brand_id','price']

#         values = df[col_names].to_dict('split')
#         values = values['data']
#         logging.info(values)

#         insert_sql = """
#                     INSERT INTO raw.facts(date,order_id,category_id,subcategory_id,brand_id,price)
#                     VALUES %s
#                     """

#         result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
#         conn_ps.commit()
    # else:
    #     None

    # return None


#########################################################
#
#   DAG Operator Setup
#
#########################################################

# import_csv_task = PythonOperator(
#     task_id='import_csv_to_postgres',
#     python_callable=import_csv_to_postgres,
#     op_args=[NSW_LGA+'NSW_LGA_CODE.csv', "NSW_LGA_CODE", conn_params],
#     dag=dag,
# )

import_load_dim_code_task = PythonOperator(
    task_id="import_load_dim_code_func",
    python_callable=import_load_dim_code_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_suburb_task = PythonOperator(
    task_id="import_load_dim_suburb_func",
    python_callable=import_load_dim_suburb_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# import_load_dim_brand_task = PythonOperator(
#     task_id="import_load_dim_brand_id",
#     python_callable=import_load_dim_brand_func,
#     op_kwargs={},
#     provide_context=True,
#     dag=dag
# )

# import_load_facts_task = PythonOperator(
#     task_id="import_load_facts_id",
#     python_callable=import_load_facts_func,
#     op_kwargs={},
#     provide_context=True,
#     dag=dag
# )

#[import_load_dim_category_task, import_load_dim_sub_category_task, import_load_dim_brand_task]  >> import_load_facts_task
#import_csv_task
import_load_dim_code_task >> import_load_dim_suburb_task
