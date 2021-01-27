from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

from datetime import datetime
from datetime import timedelta

from scripts.ticketsale.ticketsaledriver import TicketSales



start_date = datetime.now() - timedelta(days=1)
ticketSales = TicketSales()
file_path = "/usr/local/airflow/scripts/data/third_party_sales_1.csv"#os.environ["SALES_PATH"]

WORKFLOW_SCHEDULE_INTERVAL = None

default = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": start_date,
    "retries": 1,
}

#inverval order: minute hour day month day_of_week
dag = DAG(
    "ticket_event",
    description="Data pipeline mini-project",
    schedule_interval=None,
    default_args=default,
    catchup=False,
)


t1 = PythonOperator(
    task_id='extract_load',
    python_callable=ticketSales.load_third_party,
    op_kwargs={'file_path': file_path},
    dag=dag,
)
t2 = PythonOperator(
    task_id='query1',
    python_callable=ticketSales.best_selling_event,
    dag=dag,
)


t1 >> t2