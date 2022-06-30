from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id='dag',
    schedule_interval='@daily',
    start_date=days_ago(1)
  )

task_1 = BashOperator(
  task_id='remove_logs',
  bash_command='rm -r /root/airflow/logs/dag; exit 99;',
  dag=dag
    )