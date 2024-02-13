from datetime import timedelta
from airflow import DAG
from airflow.operators import PythonOperator
from kafka import KafkaProducer

from datetime import datetime
import time,json
from ..producer import producer
from ..customer import customer
from ..shop_owner import shop_owner

with DAG(
    dag_id = "florist_order_dag",
    schedule_interval = "@daily",
    default_args={
        "owner": "airflow",
        "retires": "1",
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2020,11,1),
    },
    catchup=False
) as dag:
    producer_execute = PythonOperator(
            task_id="producer_execute",
            python_callable=producer,
    )
    customer_execute = PythonOperator(
            task_id="customer_execute",
            python_callable=customer,
    )
    shop_owner_execute = PythonOperator(
            task_id="shop_owner_execute",
            python_callable=shop_owner,
    )

    producer_execute >> [shop_owner_execute,customer_execute]
