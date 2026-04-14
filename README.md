# airflow-dags

Airflow DAG repository synced via GitHub. Airflow version: **2.10.5**

## DAGs

### `simple_test_dag`

GitHub sync 동작 확인용 기본 DAG.

| 항목     | 값               |
| -------- | ---------------- |
| Schedule | `@daily`         |
| Tags     | `test`, `simple` |

**Task 구성**

1. `print_hello` — 헬로 메시지 출력
2. `print_date` — 실행 날짜 출력
3. `compute_sum` — 숫자 리스트 합산 (TaskFlow API)
4. `print_result` — 최종 결과 출력

---

### `kubernetes_pod_test_dag`

KubernetesPodOperator로 실제 Pod을 띄워 동작을 검증하는 DAG.

| 항목     | 값                                         |
| -------- | ------------------------------------------ |
| Schedule | `@daily`                                   |
| Tags     | `test`, `kubernetes`                       |
| Provider | `apache-airflow-providers-cncf-kubernetes` |

**Task 구성**

1. `echo_hello` — `busybox:1.36` 컨테이너에서 echo + date 실행
2. `run_python` — `python:3.11-slim` 컨테이너에서 Python 버전/플랫폼 정보 출력

**설정 전 확인사항**

- `namespace`: 두 KubernetesPodOperator 모두 `namespace="airflow"` 로 설정되어 있음. 실제 네임스페이스에 맞게 변경 필요.
- `in_cluster=True`: Airflow가 쿠버네티스 클러스터 내부에서 동작할 때 사용. 외부 실행 시 `False` 로 변경하고 `config_file` 경로 지정 필요.
