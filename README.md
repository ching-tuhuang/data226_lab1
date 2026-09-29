# DATA226 Lab 1

This project uses Airflow, Open-Meteo, Snowflake, and dbt to build a weather-data ETL and ELT pipeline.

## 1. Clone the repository

If you have not downloaded the project yet, run:

```bash
git clone https://github.com/ching-tuhuang/data226_lab1.git
cd data226_lab1
```

If the project already exists on your computer, update it with:

```bash
git pull origin main
```

## 2. Start Airflow

From the project root directory, run:

```bash
docker compose -f docker-compose.yaml up -d
```

This starts the Airflow services in Docker.

## 3. Configure the Snowflake connection

First, put your private key under keys folder.

Open the Airflow web UI:

```text
http://localhost:8080
```

In Airflow:

1. Open **Admin Connections**.
2. Find or create the connection with the ID:

   ```text
   snowflake_conn
   ```

3. Enter your own Snowflake connection information.
4. Save the connection.

The private key file must also be available to the Airflow container through the `keys/` folder and Docker volume configuration. Do not commit the private key to GitHub.

## 4. Configure Airflow Variables

The `lab1.py` DAG reads the coordinates from Airflow Variables. In Airflow:

1. Open **Admin  Variables**.
2. Create or update these four variables:

   ```text
   latitude1 : 56.25
   longitude1 : 38.8951
   latitude2 : -5.2833
   longitude2 : -77.0364
   ```
latitude1 longitude1 is for NY, latitude1 longitude1 is for Washington DC
## 5. Run the weather ETL DAG

Before running the dag, make sure the the database and shcema has been create in your snowflake.

In the Airflow web UI, find and trigger the DAG from:

```text
dags/lab1.py
```

This DAG:

1. Gets the latitude and longitude values from Airflow Variables.
2. Retrieves 7 days of weather forecast data from Open-Meteo.
3. Transforms the API response into raw weather records.
4. Loads the raw data into Snowflake:

   ```text
   LAB1.RAW.WEATHER_DATA_LAB1
   ```

Run this DAG first because the dbt project uses this raw table as its input.

## 6. Run the dbt ELT DAG

After the weather ETL DAG succeeds, trigger the dbt DAG from:

```text
dags/dbt_elt.py
```

This DAG runs the dbt ELT workflow. It builds the analytics models, runs the dbt tests, and creates or updates the snapshots.

The dbt project reads the raw Snowflake table and creates analytics objects in:

```text
LAB1.ANALYTICS
```

## 7. Check the results

Open the SQL file:

```text
show result.sql
```

Copy the SQL statements into a Snowflake Worksheet and execute them to verify that:

- The raw weather table was loaded successfully.
- The dbt analytics models were created successfully.
- The calculated weather metrics contain data.
- The dbt snapshot tables were created or updated.

## 8. Recommended execution order


```text
lab1.py (Airflow)-> dbt_elt.py (Airflow)-> result.sql (on snowflake)
```

## 9. Important files

```text
dags/lab1.py              Airflow weather ETL DAG
dags/dbt_elt.py           Airflow dbt ELT DAG
dbt/                      dbt project
keys/                     Snowflake private key files; do not commit
docker-compose.yaml       Docker and Airflow configuration
show result.sql           SQL statements for checking the results
```
