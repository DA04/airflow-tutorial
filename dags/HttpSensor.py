import airflow
from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor


dag = DAG(
  dag_id='dag',
  schedule_interval='@daily',
  start_date=airflow.utils.dates.days_ago(1)
  )


def response_check(response, task_instance):
  if int(response.text) == 5:
    return True
  else:
    return False

sensor = HttpSensor(
    task_id='http_sensor',
    http_conn_id='http_default',
    endpoint='integers/?num=1&min=1&max=5&col=1&base=10&format=plain',
    response_check=response_check,
    poke_interval=10,
    timeout=60,
    dag=dag)