"""
traditional_operators_dag.py
-----------------------------
The same ETL pipeline re-implemented using classic Airflow operators
and the PythonOperator instead of the TaskFlow decorator API.

Pipeline:
    extract  →  transform  →  load

This style predates Airflow 2.0 and is still widely used.  Key differences
from the TaskFlow version:
  - The DAG object is created explicitly with `with DAG(...)`.
  - Each task is an instance of an Operator class.
  - Data is passed between tasks via XCom push/pull — manual and explicit.
  - Dependencies are declared with the '>>' (bit-shift) operator.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Default arguments inherited by every task in this DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Python callables (the actual business logic)
# Each function receives **context which contains the TaskInstance (ti)
# used to push / pull XCom values.
# ---------------------------------------------------------------------------

def extract_fn(**context) -> None:
    """
    Simulate pulling raw records from an upstream data source.

    The result is pushed into XCom under the key 'raw_data' so the
    downstream transform task can retrieve it.
    """
    raw_data = {
        "source": "user_scores_api",
        "pulled_at": str(datetime.utcnow()),
        "users": [
            {"id": 1, "name": "Alice",   "score": 82},
            {"id": 2, "name": "Bob",     "score": 74},
            {"id": 3, "name": "Carol",   "score": 91},
            {"id": 4, "name": "David",   "score": 58},
            {"id": 5, "name": "Eve",     "score": 95},
        ],
    }
    print(f"[extract] Pulled {len(raw_data['users'])} records from '{raw_data['source']}'")

    # Manually push to XCom
    context["ti"].xcom_push(key="raw_data", value=raw_data)


def transform_fn(**context) -> None:
    """
    Pull raw data from XCom, apply business logic, push result.

    Business logic:
      - Keep only users with score > 70
      - Add a 'grade' label (A / B)
    """
    # Manually pull from XCom — must reference upstream task_id and key
    raw_data = context["ti"].xcom_pull(task_ids="extract", key="raw_data")

    filtered = [
        {**user, "grade": "A" if user["score"] >= 90 else "B"}
        for user in raw_data["users"]
        if user["score"] > 70
    ]

    result = {
        "processed_at": str(datetime.utcnow()),
        "total_input": len(raw_data["users"]),
        "total_output": len(filtered),
        "records": filtered,
    }

    print(
        f"[transform] {result['total_input']} in → "
        f"{result['total_output']} out after filtering"
    )
    for r in filtered:
        print(f"  {r['name']:10s} score={r['score']}  grade={r['grade']}")

    context["ti"].xcom_push(key="transformed_data", value=result)


def load_fn(**context) -> None:
    """
    Pull transformed data from XCom and write to destination.
    """
    transformed_data = context["ti"].xcom_pull(
        task_ids="transform", key="transformed_data"
    )
    records = transformed_data["records"]

    print(
        f"[load] Writing {len(records)} record(s) "
        f"(processed at {transformed_data['processed_at']}) …"
    )
    for rec in records:
        # Replace with your actual write logic:
        #   db.execute("INSERT INTO user_grades VALUES (%s, %s, %s)", ...)
        print(f"  → Inserted: {rec}")
    print("[load] Done.")


# ---------------------------------------------------------------------------
# DAG definition — explicit context-manager style
# ---------------------------------------------------------------------------
with DAG(
    dag_id="traditional_operators_etl_pipeline",
    description="ETL pipeline using classic PythonOperator + XCom",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "traditional", "operators"],
) as dag:

    # Task instances — each wraps one Python callable
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_fn,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_fn,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_fn,
    )

    # Declare execution order explicitly with the '>>' operator
    extract_task >> transform_task >> load_task
