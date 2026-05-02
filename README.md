# ✈️ Airflow DAG Demo — TaskFlow API vs Traditional Operators

A self-contained reference project that walks you through building, running, and observing Apache Airflow ETL pipelines using **Docker**. Two versions of the same pipeline are provided side-by-side so you can compare the modern TaskFlow decorator style against the classic operator style.

---

## 📁 Repository Structure

```
airflow-dag-demo/
├── dags/
│   ├── taskflow_dag.py               # Modern: @dag / @task decorators
│   └── traditional_operators_dag.py  # Classic: DAG() + PythonOperator
├── docker-compose.override.yml       # Mounts ./dags into every container
├── .env.example                      # Template for AIRFLOW_UID
├── .gitignore
└── README.md                         ← you are here
```

---

## 🏗️ Data Pipeline Architecture

### What is a DAG?

A **DAG** (Directed Acyclic Graph) is the core unit of work in Airflow. It describes *what* to run, *in what order*, and *when*. "Directed" means tasks have a clear direction of execution; "Acyclic" means there are no circular dependencies — execution always flows forward.

```
  [Extract]  →  [Transform]  →  [Load]
```

### The ETL Pattern

Both DAGs in this project implement a classic **ETL** (Extract → Transform → Load) pipeline:

| Stage | Responsibility | Real-world analogy |
|---|---|---|
| **Extract** | Pull raw data from a source | API call, DB query, file download |
| **Transform** | Apply business logic, filter, enrich | Score filtering, grade labelling |
| **Load** | Write cleaned data to a destination | Insert to data warehouse, write to S3 |

### Key Architectural Concepts

**Scheduler** — Airflow's scheduler continuously watches DAG files, determines which tasks are due to run, and places them on a queue.

**Worker** — Picks tasks off the queue and executes them. In Docker Compose mode this uses Celery with Redis as the broker.

**XCom (Cross-Communication)** — Airflow's mechanism for passing small data between tasks. Under the hood it serialises the value to the metadata database and retrieves it in the downstream task. Best for small payloads (< 48 KB); use external storage (S3, GCS) for large datasets.

**Metadata Database (Postgres)** — Stores DAG definitions, task states, run history, and XCom values.

```
┌─────────────────────────────────────────────────────────────────┐
│                         Airflow Cluster                         │
│                                                                 │
│  ┌──────────┐   schedules   ┌───────────┐   queues   ┌───────┐  │
│  │Scheduler │ ────────────► │   Redis   │ ─────────► │Worker │  │
│  └──────────┘               └───────────┘            └───────┘  │
│       │                                                   │     │
│       │  reads/writes                        reads/writes │     │
│       ▼                                                   ▼     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  Postgres (Metadata DB)                  │   │
│  │          DAG runs · task states · XCom values            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────┐                                               │
│  │  Webserver   │  ← http://localhost:8080                      │
│  └──────────────┘                                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🆚 TaskFlow API vs Traditional Operators

The two DAGs implement *identical* business logic. The only difference is style.

### Traditional Operators (`traditional_operators_dag.py`)

The pre-Airflow-2.0 pattern. Verbose but explicit.

```python
# 1. Create DAG object explicitly
with DAG(dag_id="...", schedule="@daily", ...) as dag:

    # 2. Wrap each callable in PythonOperator
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_fn,   # ← function defined separately
    )

    # 3. Push data to XCom manually inside the callable
    def extract_fn(**context):
        context["ti"].xcom_push(key="raw_data", value=data)

    # 4. Pull XCom manually in the downstream callable
    def transform_fn(**context):
        raw = context["ti"].xcom_pull(task_ids="extract", key="raw_data")

    # 5. Wire dependencies with the >> operator
    extract_task >> transform_task >> load_task
```

**When used:**
- Maintaining legacy Airflow 1.x code
- Using non-Python operators (BashOperator, S3CopyObjectOperator, etc.)


---

### TaskFlow API (`taskflow_dag.py`)

The modern pattern introduced in Airflow 2.0. Less boilerplate, more Pythonic.

```python
# 1. Decorate a function to become the DAG
@dag(dag_id="...", schedule="@daily", ...)
def my_pipeline():

    # 2. Decorate each function to become a task
    @task()
    def extract() -> dict:
        return data          # ← return value auto-pushed to XCom

    @task()
    def transform(raw_data: dict) -> dict:  # ← auto-pulled from XCom
        ...

    # 3. Wire by calling the functions — no >> needed
    raw = extract()
    processed = transform(raw)
    load(processed)

# 4. Instantiate the DAG
my_pipeline()
```

**When used:**
- All-Python pipelines on Airflow 2.0+
- You want cleaner, more testable code
- You prefer implicit XCom over manual push/pull

---

### Side-by-Side Comparison

| Feature | Traditional Operators | TaskFlow API |
|---|---|---|
| DAG creation | `with DAG(...) as dag:` | `@dag` decorator |
| Task creation | `PythonOperator(task_id=..., python_callable=fn)` | `@task` decorator on the function |
| XCom passing | Manual `xcom_push` / `xcom_pull` | Automatic via return values |
| Dependency declaration | `task_a >> task_b` | Inferred from function call chain |
| Code volume | More boilerplate | ~40% less code |
| Airflow version | 1.x and 2.x | 2.0+ only |
| Testability | Requires Airflow context mock | Plain Python function, easy to unit test |

---

## 🚀 Running the Project

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) ≥ 20.10
- [Docker Compose](https://docs.docker.com/compose/) ≥ 2.0

### Step 1 — Clone the repo

```bash
git clone https://github.com/<your-username>/airflow-dag-demo.git
cd airflow-dag-demo
```

### Step 2 — Set the Airflow UID

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

> On Windows, skip this step or set `AIRFLOW_UID=50000` manually in `.env`.

### Step 3 — Fetch the official Docker Compose file

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
```

### Step 4 — Initialise the metadata database

```bash
docker compose up airflow-init
```

Wait until the container exits with code `0`.

### Step 5 — Start all services

```bash
docker compose up -d
```

Services started:

| Container | Role | Port |
|---|---|---|
| `airflow-webserver` | Serves the UI | `8080` |
| `airflow-scheduler` | Discovers & schedules DAGs | — |
| `airflow-worker` | Executes tasks | — |
| `postgres` | Metadata database | `5432` |
| `redis` | Task queue broker | `6379` |

### Step 6 — Verify everything is healthy

```bash
docker compose ps
```

All services should show `healthy`.

### Step 7 — Open the UI

Navigate to **http://localhost:8080**

```
Username: airflow
Password: airflow
```

You should see both DAGs listed:
- `taskflow_etl_pipeline`
- `traditional_operators_etl_pipeline`

---

## 🎛️ Triggering & Observing

### Trigger via UI

1. Toggle the DAG to **On** using the slider on the left.
2. Click the **▶ Trigger DAG** button on the right.
3. Click the DAG name → **Grid view** to watch tasks turn green.
4. Click any task cell → **Logs** to see `print()` output.

### Trigger via CLI

```bash
# TaskFlow DAG
docker compose exec airflow-scheduler \
  airflow dags trigger taskflow_etl_pipeline

# Traditional DAG
docker compose exec airflow-scheduler \
  airflow dags trigger traditional_operators_etl_pipeline
```

### Test a single task

```bash
# Syntax: airflow tasks test <dag_id> <task_id> <execution_date>
docker compose exec airflow-scheduler \
  airflow tasks test taskflow_etl_pipeline extract 2024-01-01
```

### Watch logs in real-time

```bash
# Scheduler — DAG parsing, task scheduling events
docker compose logs -f airflow-scheduler

# Worker — actual task execution output
docker compose logs -f airflow-worker
```

### Inspect XCom values

In the UI: **Admin → XCom** — you can browse every value pushed between tasks.

### List import errors

If a DAG is missing from the UI:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

---

## 🧹 Teardown

```bash
# Stop containers, keep volume data
docker compose down

# Full reset — deletes all data including DB state
docker compose down -v --remove-orphans
```
---
## Visuals
__TaskAPI and traditional operators dag runs__

<img width="1867" height="1047" alt="Dags runs" src="https://github.com/user-attachments/assets/4ec03cdf-88c3-414b-93cc-9278c9461642" />

---

__Airflow task view: TaskAPI pipeline__

<img width="1868" height="1052" alt="image" src="https://github.com/user-attachments/assets/a1a128ab-a87e-44a3-b64d-a021c9df81a9" />

---

__Airflow task view: Traditional operators__

<img width="1868" height="1051" alt="Screenshot 2026-04-30 141433" src="https://github.com/user-attachments/assets/d07be439-cece-480f-9ddf-cf8eb5fcdb58" />

---

## 📖 Further Reading

- [Airflow TaskFlow API docs](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Airflow PythonOperator docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html)
- [Airflow XCom docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

---

## 📝 License

MIT — use freely, modify boldly.
