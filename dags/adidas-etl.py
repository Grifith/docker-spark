from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"
spark_master = "spark://spark:7077"
spark_app_name = "Adidas-etl"
file_path = "/usr/local/spark/resources/data/airflow.cfg"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="adidas-etl",
        description="ETL processsing for Adidas",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

file_info={}
file_info['metadata'] = ['/usr/local/spark/resources/data/ol_cdump.json','book_dump']
file_download_command ='wget https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json -O /usr/local/airflow/ol_cdump.json'
file_download_task = BashOperator(task_id="download_file", bash_command=file_download_command, dag=dag)
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

"""
Template driver pipeline creation . Contain Two Task
1 Download data from s3 bucket 
2 Load that json file to Target (Postgres)

If first step is failing job will stop and retry, it won't proceed to next task
Second task will overwrite the target table , since we are considering it as staging table
All these constraints are configurable as per the requirement 

"""

for i in file_info:
    filename = file_info[i][0]
    task_name = file_info[i][1]

    # spark_job = SparkSubmitOperator(
    #     task_id="spark_job",
    #     application="/usr/local/spark/app/spark_etl.py",  # Spark application path created in airflow and spark cluster
    #     name=spark_app_name,
    #     conn_id="spark_default",
    #     verbose=1,
    #     conf={"spark.master": spark_master},
    #     application_args=[filename],
    #     dag=dag)
    #
    spark_submit="spark-submit --master spark://spark:7077 --jars {pg_jar} --driver-class-path {pg_jar}".format(pg_jar=postgres_driver_jar)

    file_run=" /usr/local/airflow/dags/etl_scripts/spark_etl.py --input_file {}".format(filename)
    command =  spark_submit+file_run
    spark_job = BashOperator(
        task_id="data_load_{}".format(task_name),
        bash_command=command,
        dag=dag,
    )

    start >> file_download_task >> spark_job >> end

