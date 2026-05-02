"""
taskflow_dag.py
---------------
A simple ETL pipeline built with the Airflow TaskFlow API (decorator style).

Pipeline:
    extract  →  transform  →  load

The TaskFlow API (introduced in Airflow 2.0) lets you write DAGs as plain
Python functions decorated with @task. Return values are automatically
serialised and passed downstream via XCom — no manual push/pull needed.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task


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
# DAG definition
# ---------------------------------------------------------------------------
@dag(
    dag_id="taskflow_etl_pipeline",
    description="ETL pipeline using the TaskFlow API (@dag / @task decorators)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "taskflow", "decorators"],
)
def taskflow_etl_pipeline():
    """
    ### TaskFlow ETL Pipeline

    Demonstrates the modern Airflow decorator pattern:

    - **@dag** turns a function into a DAG definition.
    - **@task** turns a function into an Airflow task.
    - Return values flow between tasks automatically via XCom.
    - Dependencies are inferred from how function calls are chained.
    """

    # ------------------------------------------------------------------
    # EXTRACT
    # ------------------------------------------------------------------
    @task()
    def extract() -> dict:
        """
        Simulate pulling raw records from an upstream data source
        (e.g. a REST API, database query, or file drop).

        Returns a dict that Airflow serialises into XCom so the next
        task can consume it.
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
        return raw_data

    # ------------------------------------------------------------------
    # TRANSFORM
    # ------------------------------------------------------------------
    @task()
    def transform(raw_data: dict) -> dict:
        """
        Apply business logic to the raw records:
          - Keep only users with score > 70
          - Add a 'grade' label (A / B)
          - Compute summary statistics
        """
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
        return result

    # ------------------------------------------------------------------
    # LOAD
    # ------------------------------------------------------------------
    @task()
    def load(transformed_data: dict) -> None:
        """
        Simulate writing the cleansed records to a destination
        (e.g. a data warehouse, S3 bucket, or downstream API).
        """
        records = transformed_data["records"]
        print(
            f"[load] Writing {len(records)} record(s) "
            f"(processed at {transformed_data['processed_at']}) …"
        )
        for rec in records:
            # Replace this print with your actual write logic:
            #   db.execute("INSERT INTO user_grades VALUES (%s, %s, %s)", ...)
            print(f"  → Inserted: {rec}")
        print("[load] Done.")

    # ------------------------------------------------------------------
    # Wire up the pipeline
    # Task order is derived from the call chain — no '>>' needed.
    # ------------------------------------------------------------------
    raw      = extract()
    processed = transform(raw)
    load(processed)


# Instantiate the DAG (required so Airflow discovers it)
taskflow_etl_pipeline()
