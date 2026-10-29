# Gold Coast Water Connections Pipeline

A data pipeline that loads the City of Gold Coast's water service connection data into Postgres, keeps every load traceable and safe to rerun, and (in progress) cleans and models it with dbt to answer one question:

> **Which water connections are oldest, and what are they made of, so the council can plan replacements?**

**Status:** ingestion (raw layer) complete and tested. dbt cleaning and reporting in progress. See [Roadmap](#roadmap).

---

## Why this dataset

- **Real, live data.** The council maintains it in their asset system, and rows get edited over time (latest edit in my download: 23 Oct 2026). That makes "rerun without duplicating" a real problem, not a pretend one.
- **A real business question.** Old connections leak and burst. Knowing install dates and materials supports replacement planning.
- **Real mess.** Fake dates, inconsistent spelling, placeholder values and empty columns. Good for practising cleaning that would happen on the job.

**Source:** [Water Service Connection](https://data.gov.au/data/dataset/water-service-connection), City of Gold Coast open data, CC BY 4.0
**Size:** 177,710 rows, 52 columns, one row per water house connection

---

## Architecture

```mermaid
flowchart LR
    A[Council CSV<br/>177,710 rows] --> B[load_raw.py<br/>Python]
    B --> C[(raw schema<br/>Postgres)]
    C --> D[staging<br/>dbt]
    D --> E[report<br/>dbt]
    E --> F[Dashboard]

    classDef done fill:#d7efe0,stroke:#2c7a4a,color:#13232b
    classDef todo fill:#eeeeee,stroke:#999,color:#555,stroke-dasharray: 4 3
    class A,B,C done
    class D,E,F todo
```

Solid = built. Dashed = planned. Postgres runs in Docker.

| Layer | Holds | Rule |
|---|---|---|
| **raw** | exact copy of every file loaded, all columns as text | raw stores, never judges |
| **staging** | typed, consistent, de-duplicated data from the latest load | *in progress* |
| **report** | small tables that answer one question each | *planned* |

---

## How the loader works

`load/load_raw.py` runs these steps inside **one database transaction**:

1. **Fingerprint the file.** SHA-256 hash of the CSV, read in 1MB chunks so file size never matters.
2. **Check `raw.load_log`.** If that hash has been loaded before, print `already loaded, skipping` and stop.
3. **Record the load.** Insert a row into `raw.load_log` and get back the generated `load_id`.
4. **Guard the columns.** Stop if the header doesn't have exactly 52 columns.
5. **Bulk load.** Stream every row into `raw.water_service_connection` with Postgres `COPY`, tagging each row with its `load_id`.
6. **Record the row count** in `raw.load_log`.
7. **Commit.** Any error at any step rolls the whole load back.

### Tables

**`raw.load_log`**: one row per load (metadata)

| column | type | notes |
|---|---|---|
| load_id | serial | primary key |
| file_name | text | |
| file_hash | text | not null, unique |
| row_count | integer | |
| loaded_at | timestamptz | defaults to now() |

**`raw.water_service_connection`**: the data, one row per connection per load

- all 52 source columns as `text`
- `load_id integer not null`, foreign key to `raw.load_log`
- `loaded_at timestamptz`

---

## Design decisions

**Every raw column is `text`.** Raw's job is to accept whatever the source sends. If `gis_startup_date` were a `date` type, one bad value would fail the whole load. Types get enforced in staging, where bad values can be flagged instead of lost. It also keeps IDs like `000000007000000871` intact, since a number type would drop the leading zeros.

**A file hash decides "already loaded".** Rerunning the same file must not duplicate rows, but a new download from the council should load as a new snapshot. The hash tells those apart. `unique` on `file_hash` backs up the Python check at the database level.

**One transaction per load.** If the load fails halfway, the half-written rows and the `load_log` entry are both rolled back. Without this, a failed load would sit in the log as "done" and every rerun would skip it. I hit real failures during development and confirmed they rolled back cleanly.

**Raw keeps history.** The source is live, so re-downloading later won't give back today's version. Each load is kept and tagged with its `load_id`. Staging reads the latest one.

**No primary key on the raw data table.** `(load_id, sapobjectid)` should be unique, but raw shouldn't reject source rows. Uniqueness will be tested in dbt instead of enforced in raw.

**Column-count guard.** `COPY` matches values to columns by position. If the council adds or reorders a column, values would slide into the wrong columns without any error. The loader stops instead.

**`sapobjectid` over `objectid`.** `objectid` is a row number from the export tool and can change between exports. `sapobjectid` comes from the council's asset system, so it's the real ID.

**Port 5433.** A local Windows Postgres install already uses 5432, so Docker maps `5433` on the laptop to `5432` in the container.

---

## Data quality issues found

Found while profiling the source file. These are what the staging layer is being built to handle:

- 5 connections with an install date in the year **1800**
- Large spikes of install dates on **1 July** (e.g. 5,939 on 1/7/1989), likely estimated financial-year dates rather than real ones
- **22,970** rows with `TBA` as the pipe class instead of blank
- Inconsistent casing: `UNLINED` (156,433) vs `Unlined` (456)
- About **10 columns completely empty**, including every relining column
- **49** connections with a length of 0

---

## How to run

**Needs:** Docker Desktop, Python 3.10+

1. **Get the data.** Download the Water Service Connection CSV from the source link and save it as `data/Water_Service_Connection.csv`. (Data isn't committed to the repo.)

2. **Start Postgres**
   ```
   docker compose up -d
   ```

3. **Create the raw tables**

   PowerShell:
   ```
   Get-Content sql/01_raw.sql | docker compose exec -T db psql -U de -d water -v ON_ERROR_STOP=1
   ```
   macOS / Linux:
   ```
   docker compose exec -T db psql -U de -d water -v ON_ERROR_STOP=1 < sql/01_raw.sql
   ```

4. **Set up Python**
   ```
   python -m venv .venv
   .venv\Scripts\activate          # Windows
   source .venv/bin/activate       # macOS / Linux
   pip install -r requirements.txt
   ```

5. **Load**
   ```
   python load/load_raw.py
   ```

6. **Check it**
   ```
   docker compose exec db psql -U de -d water -c "select load_id, count(*) from raw.water_service_connection group by load_id;"
   ```

Run step 5 again and it prints `already loaded, skipping`. The count stays at 177,710.

> Credentials in `docker-compose.yml` are for local development only.

---

## Project structure

```
tba_1/
├── data/                   # source CSV (git-ignored)
├── load/
│   └── load_raw.py         # raw loader
├── sql/
│   └── 01_raw.sql          # raw schema and tables
├── water_dbt/              # dbt project (in progress)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Roadmap

- [x] Postgres in Docker with persistent storage
- [x] Raw schema with load tracking
- [x] Idempotent, transactional Python loader with column guard
- [x] dbt installed and connected
- [ ] dbt staging model: types, consistent text, NULLs for placeholders, flagged bad dates
- [ ] dbt report models: connections by install decade, oldest connections by material and water zone (window functions, CTEs)
- [ ] dbt tests: unique and not-null IDs, accepted values, no future install dates
- [ ] Power BI dashboard
- [ ] One-command run (loader and dbt in Docker)
- [ ] Scheduled loads straight from the council's API

---

## Author

Paul CJ · [GitHub](https://github.com/pxul1236) · [LinkedIn](https://www.linkedin.com/in/paul-cj)