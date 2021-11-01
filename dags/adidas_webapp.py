"""
This is a sample airflow code which will download given file at regular intervals . If files are downloaded it move to next step
Loading to Postgres(Target) . For the docker we can go ahead with the Postgres implementation it can be easily changed accoring to the config
We are parsing downloaded file using pyspark . After cleaning up the same we are writing that to a staging table in postgres
In real time ETL pipeline scenario there will be more steps involved to slice and dice the data further for the last step
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 26),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("adidas-webapp", default_args=default_args, schedule_interval="30 23 * * *",catchup=False)

running_webserver ='python /usr/local/airflow/dags/flask_app.py'
stopping_webserver = ' pkill -9 -f /usr/local/airflow/dags/flask_app.py '

# t1, t2 and t3 are examples of tasks created by instantiating operators
start_webserver_task = BashOperator(task_id="start_webserver_task", bash_command=running_webserver, dag=dag)

stop_webserver_task = BashOperator(task_id="stop_webserver_task",
                                   bash_command=stopping_webserver,
                                   trigger_rule=TriggerRule.ONE_FAILED,
                                   dag=dag)


stop_webserver_task.set_upstream(start_webserver_task)