# ETL MLOps Task 3: Automated Currency Exchange ETL Pipeline

An end-to-end automated ETL pipeline built using **Astronomer (Astro) CLI** for **Apache Airflow**, extracting daily currency exchange rates from the Frankfurter API, standardizing the data into JSON records, and loading them into **MongoDB** with web administration via **Mongo Express**.

---

## Architecture Overview

- **Orchestration**: Apache Airflow via Astro CLI (Docker-based runtime)
- **Data Source**: [Frankfurter API v2](https://frankfurter.dev/) (`EUR` base quotes for `USD`, `GBP`, `JPY`)
- **Data Transfer**: Airflow Named XCom (`ti.xcom_push` and `ti.xcom_pull`)
- **Storage**: MongoDB (`currency_db.exchange_rates`) with persistent Docker volume (`mongodb_data`)
- **Database UI**: Mongo Express exposed on `localhost:8081`
- **Networking**: Custom shared Docker bridge network (`airflow`) connecting Airflow workers, MongoDB, and Mongo Express

---

## Project Structure

```text
.
├── .astro/                     # Astro CLI project configuration
├── dags/                       # Airflow DAG definitions
│   └── currency_exchange_etl.py # 3-task ETL DAG (Extract, Transform, Load)
├── docs/                       # Project documentation & screenshots
│   ├── Task 3 Building Airflow ETL Pipeline.pdf
│   └── screenshots/            # Execution and verification screenshots
├── include/                    # Additional project assets/scripts
├── plugins/                    # Custom Airflow plugins
├── tests/                      # Pytest DAG integrity and unit tests
├── .dockerignore               # Docker ignore rules
├── .env                        # Environment variables (local-only)
├── .gitignore                  # Git ignore rules
├── Dockerfile                  # Astro runtime base image definition
├── docker-compose.override.yml # MongoDB & Mongo Express service overrides
├── packages.txt                # OS-level dependencies
├── requirements.txt            # Python dependencies (pymongo, requests)
└── README.md                   # Project overview & documentation
```

---

## Quickstart

### Prerequisites
- Docker Desktop (running)
- Astro CLI (`winget install -e --id Astronomer.Astro`)

### Launch Environment
```bash
astro dev start
```

- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (admin / admin)
- **Mongo Express UI**: [http://localhost:8081](http://localhost:8081) (admin / pass)
