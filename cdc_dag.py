from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='pyspark_cdc_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily'
) as dag:
    
    # Define the SparkSubmitOperator
    run_pyspark_job = SparkSubmitOperator(
        task_id='run_cdc_job',
        application='/opt/bitnami/spark/cdc-data.py',  
        conn_id='spark_default',   ??this would be the connection details that we setup in airflow.
        conf={'spark.master': 'spark://your-spark-master-url:7077'},  
    )

    run_cdc_job
