# 🚀 Airflow Producer–Consumer Batch ETL Pipeline

[![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

## 🔹 Overview
This project demonstrates a **production-style batch ETL pipeline** built using Apache Airflow. By implementing a **Producer–Consumer architecture**, the system decouples data generation from data processing, ensuring high reliability and scalability.

The pipeline automates the ingestion of raw user data, applies necessary transformations, and loads it into a PostgreSQL warehouse with strict adherence to **idempotency** and **failure safety** principles.

---

## 🏗 Architecture & Logic

### 1. Producer DAG (`producer_users_daily`)
* **Role**: Data Ingestion.
* **Action**: Generates a `users.csv` file for the specific `ds` (logical date).
* **Storage**: Files are organized by date: `/opt/airflow/data/raw/users/{ds}/users.csv`.

### 2. Consumer DAG (`consumer_users_daily`)
* **Task 1: Synchronization**: Uses `ExternalTaskSensor` to wait for the Producer DAG's success.
* **Task 2: Validation**: Uses `FileSensor` (Reschedule mode) to confirm the raw file exists.
* **Task 3: Transformation**: Python-based schema cleanup, type casting, and `ds` enrichment.
* **Task 4: Load**: Idempotent **Delete + Insert** into the `users_daily` table.

---

## 🧠 Key Engineering Concepts

> [!TIP]
> **Idempotency is the core of this pipeline.** Running the same DAG for the same date multiple times will always yield the same result without duplicating data.

* **Producer–Consumer Pattern**: Decouples the source availability from the processing logic.
* **Logical Date (ds) Driven**: No `datetime.now()` usage; all tasks are deterministic based on the Airflow logical date.
* **Worker Efficiency**: Sensors utilize `reschedule` mode, releasing worker slots while waiting.
* **Backfill Ready**: `catchup=True` allows for safe historical data re-runs.

---

## 🛡 Data Safety & Failure Handling

| Scenario | System Behavior | Safety Mechanism |
| :--- | :--- | :--- |
| **Upstream Delay** | Consumer stays in "Upstream Failed/Waiting" | `ExternalTaskSensor` |
| **Missing File** | Sensor retries periodically (low resource usage) | `FileSensor` |
| **Interrupted Load** | Partial data is never committed | **Atomic DB Transactions** |
| **Duplicate Entry** | Database rejects the row to prevent corruption | `UNIQUE (id, dt)` Constraint |

---

## 🚀 How to Run

### 1. Environment Setup
Deploy the stack using Docker Compose:
```bash
docker-compose up -d
```
### 2. Configure Airflow Connections
Access the Airflow UI at localhost:8080 and set up the following:

  * postgres_default: Connection string for your PostgreSQL instance.
  
  * fs_default: Path to the local data volume.

### 3. Execution Flow
* Unpause and trigger producer_users_daily.

* The consumer_users_daily will automatically wake up upon producer completion and file detection.

## 📈 Roadmap & Future Improvements
[ ] Distributed Compute: Integrate Apache Spark for heavy transformations.

[ ] Streaming: Implement Kafka for event-driven data arrival.

[ ] Data Quality: Add Great Expectations for automated schema validation.

[ ] Cloud Migration: Move local storage to AWS S3 or Google Cloud Storage.

## 👤 Author
### Ayush Nagras
