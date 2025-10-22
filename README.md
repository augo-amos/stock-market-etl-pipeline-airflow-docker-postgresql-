# Stock Market ETL Pipeline (Airflow + Docker + PostgreSQL)

This project is a **real-time stock market ETL pipeline** that fetches, transforms, and stores stock data from the **Polygon.io API** using **Apache Airflow**, **Pandas**, and **PostgreSQL** (hosted on **Aiven Cloud**).  
The system runs in a fully containerized environment powered by **Docker Compose**.

---

## Architecture Overview

```

Polygon API → Airflow (Extract) → Pandas (Transform) → PostgreSQL (Load)

```

Airflow orchestrates three tasks:

1. **Extract:** Fetches minute-level stock data for selected tickers from Polygon.io.  
2. **Transform:** Cleans, validates, and deduplicates the dataset.  
3. **Load:** Inserts new data into the PostgreSQL `stock_data` table (append-only).

---

## Project Structure

```

├── dags/
│   └── stock_etl.py          # Airflow DAG definition
├── logs/                     # Airflow logs
├── plugins/                  # (optional) Custom operators/plugins
├── secrets/                  # Mounted read-only secrets (e.g., SSL cert)
├── .env                      # Environment variables (ignored in Git)
├── docker-compose.yml        # Dockerized Airflow setup
└── README.md

````

---

## Environment Variables

Create a `.env` file in your project root:

```bash
# Polygon API
API_KEY=your_polygon_api_key

# Aiven PostgreSQL connection
AIVEN_DB_HOST=your-db-host.aivencloud.com
AIVEN_DB_PORT=your-port
AIVEN_DB_NAME=your-db-name
AIVEN_DB_USER=your-username
AIVEN_DB_PASSWORD=your-password
AIVEN_SSLMODE=require
AIVEN_CA_CERT_PATH=/opt/airflow/secrets/ca.pem

# Airflow settings
AIRFLOW__CORE__FERNET_KEY=your_fernet_key
STOCK_TICKERS=NVDA,TSLA,MSFT,GOOGL
````

> **Important:**
>
> * Do **not** commit `.env` to GitHub.
> * The `.env` file is automatically used by all Docker services via the `env_file` directive.

### Example `.gitignore`

```bash
# Ignore environment and cache files
.env
__pycache__/
logs/
*.pyc
```

---

## Running the Project

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/stock-etl-airflow.git
cd stock-etl-airflow
```

### 2. Initialize Airflow

```bash
docker compose up airflow-init
```

### 3. Start Airflow services

```bash
docker compose up -d
```

### 4. Access Airflow UI

Visit: [http://localhost:8080](http://localhost:8080)
Default credentials:

```
Username: admin
Password: admin
```

### 5. Trigger the DAG

* Log in to Airflow
* Unpause the `stock_etl` DAG
* Click **Trigger DAG** to start data loading

---

## Key Features

* Hourly stock data ingestion (`@hourly` schedule)
* Automated data cleaning and deduplication
* Secure PostgreSQL SSL connection
* Append-only loading strategy
* Fully containerized setup via Docker Compose
* Modular ETL design (Extract → Transform → Load)

---

## Tech Stack

| Component          | Technology               |
| ------------------ | ------------------------ |
| **Orchestration**  | Apache Airflow           |
| **Data Source**    | Polygon.io API           |
| **Transformation** | Pandas                   |
| **Database**       | PostgreSQL (Aiven Cloud) |
| **Deployment**     | Docker Compose           |
| **Language**       | Python 3.9               |

---

## Troubleshooting

**Rebuild from scratch:**

```bash
docker compose down --volumes --remove-orphans
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

**View logs:**

```bash
docker logs airflow_webserver
docker logs airflow_scheduler
```

**Stop all containers:**

```bash
docker compose down
```

---

## License

This project is licensed under the **MIT License**.

