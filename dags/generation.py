from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

def create_dag(dag_id,
               dag_number,
               default_args,
               schedule='@daily'):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        task_list=list()
        for i in range(0, 10):
          task_list.append(DummyOperator(task_id=f'task_{i}', dag=dag))
        [task_list]

    return dag


for n in range(1, 6):
    dag_id = f'dag_{n}'

    default_args = {'owner': 'airflow', 'start_date': days_ago(1)}
    dag_number = n
    
    globals()[dag_id] = create_dag(dag_id, dag_number, default_args)