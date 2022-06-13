from airflow import DAG
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine
from airflow.operators.bash_operator import BashOperator

def insert_data_order_detail():
    order_detail = pd.read_csv('/usr/local/airflow/dags/data/order_detail.csv')
    insert = """
        INSERT INTO ingestion.order_detail (
            order_created_timestamp 
            , status 
            , price 
            , discount 
            , id 
            , driver_id 
            , user_id 
            , restaurant_id )
        VALUES 
    """
    mysql_hook = PostgresHook(postgres_conn_id='airflow_postgres')
    running = 1 
    for index, row in order_detail.iterrows():
        order_created_timestamp ,status ,price ,discount ,id ,driver_id ,user_id ,restaurant_id = row
        values = "('"+"','".join([str(order_created_timestamp) ,str(status) ,str(price) ,str(discount) ,str(id) ,str(driver_id) ,str(user_id) ,str(restaurant_id)])+"')"
        query = insert + values + ";"
        print ("running : "+str(query)+"\tstart")
        # print ("running : "+str(running)+"\tstart")
        mysql_hook.run(query)
        print ("running : "+str(running)+"\tend")
        running=running+1


def insert_data_restaurant_detail():
    restaurant_detail = pd.read_csv('/usr/local/airflow/dags/data/restaurant_detail.csv')
    insert = """
        INSERT INTO ingestion.restaurant_detail (
            id
            , restaurant_name
            , category
            , esimated_cooking_time
            , latitude
            , longitude
            )
        VALUES 
    """
    print ("get query ")
    list_values_restaurants = []
    for index, row in restaurant_detail.iterrows():
        id , restaurant_name , category , esimated_cooking_time , latitude , longitude = row
        values = "('"+"','".join([str(id) , str(restaurant_name) , str(category) , str(esimated_cooking_time) , str(latitude) , str(longitude)])+"')"
        if values not in list_values_restaurants:
            list_values_restaurants.append(values)
    query = insert + '\n'.join(list_values_restaurants) + ";"
    print ("get query done")
    print ("start query")
    mysql_hook = PostgresHook(postgres_conn_id='airflow_postgres')
    mysql_hook.run(query)
    print ("query done")


with DAG(dag_id="ingestion_data",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@once",
         catchup=False) as dag:

    insert_table_order_detail = PythonOperator(
        task_id="insert_table_order_detail",
        python_callable=insert_data_order_detail
    )


    # insert_table_order_detail = BashOperator(
    #     task_id="insert_table_order_detail",
    #     bash_command = (
    #         'psql -U airflow -c "'
    #         'COPY ingestion.order_detail(order_created_timestamp , status , price , discount , id , driver_id , user_id , restaurant_id )'
    #         "FROM '/usr/local/airflow/dags/data/order_detail.csv' "
    #         "DELIMITER ',' "
    #         'CSV HEADER"'
    #     )
    # )
    # insert_table_restaurant_detail = PythonOperator(
    #     task_id="insert_table_restaurant_detail",
    #     python_callable=insert_data_restaurant_detail
    #     )
    

insert_table_order_detail 
