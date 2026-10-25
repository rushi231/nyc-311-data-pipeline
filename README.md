# NYC 311 Data Pipeline

A Python ETL pipeline that fetches NYC 311 service requests, validates and cleans the data, and loads it into PostgreSQL. Prefect manages the pipeline, and a Streamlit dashboard displays request trends, complaint types, and borough performance.

## How to Run

Requires Python 3.11 and Docker Compose.

### 1. Clone and install

```bash
git clone https://github.com/rushi231/nyc-311-data-pipeline.git
cd nyc-311-data-pipeline

python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
```

Set matching database credentials in `.env` and `docker-compose.yml`.

### 2. Start the services

Before starting:
- Set the Prefect image in `docker-compose.yml` to `prefecthq/prefect:2.14.21`.
- Remove the unused `from prefect.schedules import CronSchedule` import from `orchestration/pipeline_flow.py`.

```bash
docker compose up -d
```

Wait for PostgreSQL to become healthy. The database tables are created automatically on the first start.

### 3. Run the pipeline

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
python orchestration/pipeline_flow.py
```

The default run fetches up to 10,000 requests from the last 7 days.

View pipeline runs at http://localhost:4200.

### 4. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501 to view the dashboard.
