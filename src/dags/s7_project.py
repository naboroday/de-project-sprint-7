import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'naboroday',
    'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id = "s7_project",
    default_args=default_args,
    schedule_interval=None,
)

users_mart = SparkSubmitOperator(
    task_id='users_mart',
    dag=dag_spark,
    application ='/lessons/users_mart.py' ,
    conn_id= 'yarn_spark',
    application_args = ["/user/master/data/geo/events", "/user/naboroday/project_7/timezone/geo_2.csv"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

actions_geo_mart = SparkSubmitOperator(
    task_id='actions_geo_mart',
    dag=dag_spark,
    application ='/lessons/actions_geo_mart.py' ,
    conn_id= 'yarn_spark',
    application_args = ["/user/master/data/geo/events", "/user/naboroday/project_7/geo.csv"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

friends_rec_mart = SparkSubmitOperator(
    task_id='friends_rec_mart',
    dag=dag_spark,
    application ='/lessons/friends_rec_mart.py' ,
    conn_id= 'yarn_spark',
    application_args = ["/user/master/data/geo/events", "/user/naboroday/project_7/timezone/geo_2.csv"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

users_mart
actions_geo_mart
friends_rec_mart
