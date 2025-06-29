from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append('/opt/airflow/scripts')

from etl_script import extract, transform, load

with DAG(
    dag_id='etl_csv_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    t1 >> t2 >> t3
