"""
DAG for launching a Kubernetes Pod using KubernetesPodOperator.
Requires: apache-airflow-providers-cncf-kubernetes
Airflow version: 2.10.5
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="kubernetes_pod_test_dag",
    description="Test DAG that launches a Kubernetes Pod via KubernetesPodOperator",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["test", "kubernetes"],
)
def kubernetes_pod_test_dag():
    # Task 1: run a simple echo command in a busybox container
    echo_task = KubernetesPodOperator(
        task_id="echo_hello",
        name="airflow-test-echo",
        namespace="airflow",          # change to your target namespace
        image="busybox:1.36",
        cmds=["sh", "-c"],
        arguments=["echo 'Hello from Kubernetes Pod!' && date"],
        # Use in-cluster config when running inside Kubernetes
        in_cluster=True,
        # Automatically remove the pod after completion
        on_finish_action="delete_pod",
        get_logs=True,
        # Resource requests/limits for the pod
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "128Mi"},
            limits={"cpu": "200m", "memory": "256Mi"},
        ),
        # Labels applied to the pod
        labels={"app": "airflow-test", "component": "dag-runner"},
    )

    # Task 2: run a Python script inside a python container
    python_task = KubernetesPodOperator(
        task_id="run_python",
        name="airflow-test-python",
        namespace="airflow",          # change to your target namespace
        image="python:3.11-slim",
        cmds=["python", "-c"],
        arguments=[
            (
                "import sys, platform; "
                "print(f'Python {sys.version}'); "
                "print(f'Platform: {platform.platform()}'); "
                "print('Kubernetes Pod is running successfully!')"
            )
        ],
        in_cluster=True,
        on_finish_action="delete_pod",
        get_logs=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "128Mi"},
            limits={"cpu": "500m", "memory": "512Mi"},
        ),
        labels={"app": "airflow-test", "component": "python-runner"},
        # Environment variables injected into the container
        env_vars=[
            k8s.V1EnvVar(name="ENV", value="test"),
            k8s.V1EnvVar(name="DAG_ID", value="kubernetes_pod_test_dag"),
        ],
    )

    echo_task >> python_task


kubernetes_pod_test_dag()
