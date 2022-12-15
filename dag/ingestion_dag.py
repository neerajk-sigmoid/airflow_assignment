from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime,timedelta
from airflow.models import DagRun
from airflow.models import Variable



dag_variables = Variable.get("assignment_dag_variables", deserialize_json=True)
default_args = {
    'email': dag_variables['email'],
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'email_on_retry': False
}



with DAG(dag_id="assignment_ingestion",
         start_date=datetime(2022,12,14),
         schedule_interval="@daily",
         default_args=default_args,
         max_active_runs=1,
         catchup=False) as dag:

        ingest_data_with_filter_value_1 = BashOperator(
        task_id='ingest_data_with_filter_value_1', 
        bash_command=dag_variables['command_with_base_path'] + " '{\"filter_value\": 1}'"
        )

        ingest_data_with_filter_value_2 = BashOperator(
        task_id='ingest_data_with_filter_value_2', 
        bash_command=dag_variables['command_with_base_path'] + " '{\"filter_value\": 2}'"
        )

        ingest_data_with_filter_value_3 = BashOperator(
        task_id='ingest_data_with_filter_value_3', 
        bash_command=dag_variables['command_with_base_path'] + " '{\"filter_value\": 3}'"
        )

        trigger_dependent_dag = TriggerDagRunOperator(
        task_id="show_dag1_status",
        trigger_dag_id="assignment_dag_status",
        trigger_rule="all_done"
        )


[ingest_data_with_filter_value_1,ingest_data_with_filter_value_2,ingest_data_with_filter_value_3]>> trigger_dependent_dag


def get_task_status(dag_id,dag_variables,tasks):

    last_dag_run = DagRun.find(dag_id=dag_id)
    last_dag_run.sort(key=lambda x: x.execution_date, reverse=True)

    if "success" in last_dag_run[0].get_task_instance(tasks[0]).state and "success" in last_dag_run[0].get_task_instance(tasks[1]).state \
                    and "success" in last_dag_run[0].get_task_instance(tasks[2]).state :
        print("Previous Dag status is Success")
    else:
        print("Previous Dag status is Failed")
    
    return



with DAG(dag_id="assignment_dag_status",
         start_date=datetime(2022,12,14),
         schedule_interval="@daily",
         max_active_runs=1,
         catchup=False) as dag:

        show_dag1_status = PythonOperator(
        task_id="show_dag1_status",
        python_callable=get_task_status,
        op_args = ['assignment_ingestion',dag_variables,dag_variables['tasks']],
        do_xcom_push = False)
