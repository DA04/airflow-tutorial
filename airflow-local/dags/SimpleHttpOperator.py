from airflow import DAG
from airflow.utils.dates import days_ago, timedelta

from airflow.operators.http_operator import SimpleHttpOperator

dag = DAG('_SimpleHttpOperator',schedule_interval=timedelta(days=1), start_date=days_ago(1))

task_1 = SimpleHttpOperator(
  task_id='get_value',
  method='GET',
  http_conn_id='http_default',
  endpoint='integers/?num=1&min=1&max=5&col=1&base=2&format=plain',
  dag=dag)