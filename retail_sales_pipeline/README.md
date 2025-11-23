# Retail Sales Data Pipeline

My first data engineering project - an automated ETL pipeline that processes retail sales data and loads it into a database.

## What This Does

Takes a CSV file of sales data and automatically:
1. Extracts data from the file
2. Cleans and transforms it (adds calculated fields, fixes dates)
3. Loads it into PostgreSQL database
4. Runs daily via Apache Airflow

## Tech Stack

- Python (data processing)
- PostgreSQL (database)
- Apache Airflow (scheduling)
- Docker (containerization)

## Project Structure
```
retail_sales_pipeline/
├── data/                    # CSV files
├── scripts/
│   ├── extract_data.py      # Reads CSV
│   ├── transform_data.py    # Cleans data
│   └── load_to_db.py        # Saves to database
├── dags/
│   └── retail_pipeline_dag.py  # Airflow workflow
└── docker-compose.yaml      # Docker setup
```

## Setup Instructions

**1. Install Requirements**
- Docker Desktop
- PostgreSQL
- Python 3.8+

**2. Clone and Configure**
```bash
git clone <your-repo-url>
cd retail_sales_pipeline
```

Create `.env` file:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retail_sales
DB_USER=postgres
DB_PASSWORD=your_password
```

**3. Install Python Dependencies**
```bash
pip install pandas numpy psycopg2-binary python-dotenv
```

**4. Create Database**
In pgAdmin, create database named `retail_sales`

**5. Start Airflow**
```bash
docker-compose up -d
```

Access Airflow UI at http://localhost:8080 (username/password: airflow/airflow)

**6. Run Pipeline**
In Airflow UI, toggle the DAG on and trigger it manually

## What I Learned

- Building ETL pipelines with Python
- Database design and SQL
- Workflow orchestration with Airflow
- Docker containerization
- Version control best practices

## Future Improvements

- Add data quality tests
- Implement incremental loads
- Add error notifications
- Build streaming pipeline with Kafka