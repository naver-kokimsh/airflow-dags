"""
Simple test DAG for verifying GitHub sync with Airflow.
Airflow version: 2.10.5
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


def _print_hello():
    print("Hello from Airflow! GitHub sync is working.")


def _print_date(**context):
    logical_date = context["logical_date"]
    print(f"Logical date: {logical_date}")


def _sum_numbers(numbers: list[int]) -> int:
    result = sum(numbers)
    print(f"Sum of {numbers} = {result}")
    return result


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="simple_test_dag",
    description="Simple DAG to verify GitHub sync is working",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["test", "simple"],
)
def simple_test_dag():
    # Task 1: print hello message
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=_print_hello,
    )

    # Task 2: print logical execution date
    date_task = PythonOperator(
        task_id="print_date",
        python_callable=_print_date,
    )

    # Task 3: compute sum using TaskFlow API
    @task(task_id="compute_sum")
    def compute_sum() -> int:
        numbers = [1, 2, 3, 4, 5]
        return _sum_numbers(numbers)

    # Task 4: print final result
    @task(task_id="print_result")
    def print_result(total: int):
        print(f"Pipeline complete. Final result: {total}")

    result = compute_sum()
    hello_task >> date_task >> result >> print_result(result)


simple_test_dag()
