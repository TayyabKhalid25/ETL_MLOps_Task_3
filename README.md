# Automated Currency Exchange ETL Pipeline (Airflow & Astro CLI)

An end-to-end automated ETL pipeline built using **Astronomer (Astro) CLI** for **Apache Airflow**, extracting daily currency exchange rates from the Frankfurter API, standardizing the data into structured JSON documents, and loading them into **MongoDB** with web administration via **Mongo Express**.

- **Repository**: [https://github.com/TayyabKhalid25/ETL_MLOps_Task_3](https://github.com/TayyabKhalid25/ETL_MLOps_Task_3)
- **Detailed Project Report**: [docs/Project_Report.md](docs/Project_Report.md)

---

## Architecture Overview

- **Orchestration**: Apache Airflow via Astro CLI runtime (Docker-based)
- **Data Source**: [Frankfurter API v2](https://frankfurter.dev/) (`EUR` base quotes for `USD`, `GBP`, `JPY`)
- **Data Transfer**: Airflow Named XCom (`ti.xcom_push` and `ti.xcom_pull`)
- **Storage**: MongoDB (`currency_db.exchange_rates`) with persistent Docker volume (`mongodb_data`)
- **Database UI**: Mongo Express exposed on `localhost:8081`
- **Networking**: Shared Docker bridge network (`airflow`) connecting Airflow workers, MongoDB, and Mongo Express

```text
Frankfurter API (HTTPS) ──> extract_currency_rates
                                   │ (Named XCom: 'raw_exchange_data')
                                   ▼
                            transform_exchange_rates
                                   │ (Named XCom: 'transformed_exchange_data')
                                   ▼
                            load_to_mongodb ──> MongoDB (mongodb:27017) 
                                                   │
                                                   ▼
                                            Mongo Express (localhost:8081)
```

---

## Target Document Schema

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1214,
  "rate_date": "2026-11-08",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-08T06:20:42"
}
```

---

## Project Structure

```text
.
├── .astro/                     # Astro CLI project configuration
├── dags/                       # Airflow DAG definitions
│   └── currency_exchange_etl.py # 3-task ETL DAG (Extract, Transform, Load)
├── docs/                       # Project documentation & screenshots
│   ├── Project_Report.md       # Full submission report with embedded screenshots
│   ├── Task 3 Building Airflow ETL Pipeline.pdf
│   └── screenshots/            # Execution and verification screenshots
├── include/                    # Additional project assets/scripts
├── plugins/                    # Custom Airflow plugins
├── tests/                      # Pytest DAG integrity and unit tests
│   └── dags/
│       └── test_dag_integrity.py
├── .dockerignore               # Docker ignore rules
├── .gitignore                  # Git ignore rules
├── Dockerfile                  # Astro runtime base image definition
├── docker-compose.override.yml # MongoDB & Mongo Express service overrides
├── packages.txt                # OS-level dependencies
├── requirements.txt            # Python dependencies (pymongo, requests)
└── README.md                   # Project overview & documentation
```

---

## Local Setup & Execution

### Prerequisites
- Docker Desktop (active and running)
- Astro CLI installed (`winget install -e --id Astronomer.Astro`)

### 1. Launch Services
```bash
astro dev start
```

### 2. Access Interfaces
- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Username: `admin` / Password: `admin`)
- **Mongo Express UI**: [http://localhost:8081](http://localhost:8081) (Username: `admin` / Password: `pass`)

### 3. Run Pipeline
1. In the Airflow UI, locate the DAG `currency_exchange_etl`.
2. Unpause the DAG and trigger a manual run.
3. Verify that all 3 tasks (`extract_currency_rates` -> `transform_exchange_rates` -> `load_to_mongodb`) succeed.
4. Open Mongo Express at `http://localhost:8081` and inspect the `currency_db.exchange_rates` collection.

---

## Testing

Run DAG integrity and structure tests with pytest:
```bash
astro dev pytest
```
