from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

def get_data():
  connection = BaseHook.get_connection('custom_conn_id')
  value = {'host':connection.host, 'login':connection.login, 'password':connection.password}
  return Variable.set(connection.conn_id, value, serialize_json=True)

dag = DAG('_Comnection_Variable', schedule_interval='@daily', start_date=days_ago(1))

task_1 = PythonOperator(
    task_id = 'task_1',
    python_callable=get_data,
    dag=dag
)