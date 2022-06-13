from airflow import DAG
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
from airflow.utils.dates import days_ago

# db+postgresql://airflow:airflow@postgres/airflow

def init_tbl_func():
    #print(my_query)  # 'value_1'
    my_query = """
            DROP SCHEMA IF EXISTS ingestion CASCADE;
            CREATE SCHEMA IF NOT EXISTS ingestion;

            DROP TABLE IF EXISTS ingestion.order_detail;
            CREATE TABLE IF NOT EXISTS ingestion.order_detail (
            	order_created_timestamp timestamp NOT NULL 
            	,status          text        NOT NULL 
            	,price           integer     NOT NULL 
            	,discount        float8               
            	,id              text                 
            	,driver_id       text                 
            	,user_id         text                 
            	,restaurant_id   text                
            );

            DROP TABLE IF EXISTS ingestion.restaurant_detail;
            CREATE TABLE IF NOT EXISTS ingestion.restaurant_detail (
                id                          TEXT    PRIMARY KEY    NOT NULL 
                ,restaurant_name            TEXT    NOT NULL             
                ,category                   TEXT    NOT NULL         
                ,esimated_cooking_time      float8                         
                ,latitude                   float8             
                ,longitude                  float8           
            );

            DROP TABLE IF EXISTS ingestion.order_detail_new;
            CREATE TABLE IF NOT EXISTS ingestion.order_detail_new (
            	order_created_timestamp timestamp   NOT NULL 
            	,status                  text       NOT NULL 
            	,price                   integer    NOT NULL 
            	,discount                float8               
            	,id                      text 
            	,driver_id               text 
            	,user_id                 text                 
            	,restaurant_id           text
            	,discount_no_null        integer
            );

            DROP TABLE IF EXISTS ingestion.restaurant_detail_new;
            CREATE TABLE IF NOT EXISTS ingestion.restaurant_detail_new (
                id                          TEXT    PRIMARY KEY    NOT NULL 
                ,restaurant_name            TEXT        NOT NULL             
                ,category                   TEXT        NOT NULL         
                ,esimated_cooking_time      float8                     
                ,latitude                   float8         
                ,longitude                  float8       
            	,cooking_bin                integer
    );
    
        """
    mysql_hook = PostgresHook(postgres_conn_id='airflow_postgres')
    mysql_hook.run(my_query)



with DAG(dag_id="init_table",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@once",
         catchup=False) as dag:

    initial_table = PythonOperator(
        task_id="initial_table",
        python_callable=init_tbl_func


)

initial_table
