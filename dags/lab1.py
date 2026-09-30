from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


import requests
import pandas as pd


def get_lat_and_lon():
    lat1 = Variable.get("latitude1")
    lon1 = Variable.get("longitude1")

    lat2 = Variable.get("latitude2")
    lon2 = Variable.get("longitude2")

    print("lat1,lon1 =", lat1, lon1)
    print("lat2,lon2 =", lat2, lon2)

    return lat1, lon1, lat2, lon2


def return_snowflake_conn():
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn"
    )

    conn = hook.get_conn()
    return conn.cursor()


@task
def extract():
    """Retrieves 7 days of weather forecast data"""

    url = "https://api.open-meteo.com/v1/forecast"

    lat1, lon1, lat2, lon2 = get_lat_and_lon()

    def extract_data_from_url(lat, lon):
        params = {
            "latitude": lat,
            "longitude": lon,
            "past_days": 0,
            "forecast_days": 7,
            "daily": [
                # Temperature
                "temperature_2m_mean",
                "temperature_2m_max",
                "temperature_2m_min",

                # Precipitation
                "precipitation_sum",
                "rain_sum",
                "precipitation_hours",

                # Weather
                "weather_code",

                # Sunshine
                "sunshine_duration",
                "daylight_duration",

                # Humidity and cloud cover
                "relative_humidity_2m_mean",
                "cloud_cover_mean",

                # Wind
                "wind_speed_10m_mean",
                "wind_speed_10m_max",
                "wind_gusts_10m_max",

                # Solar radiation
                "shortwave_radiation_sum",
            ],
            "timezone": "America/Los_Angeles",
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        if "daily" not in data:
            raise ValueError(
                f"API response does not contain daily data: {data}"
            )

        return data

    data1 = extract_data_from_url(lat1, lon1)
    data2 = extract_data_from_url(lat2, lon2)

    return data1, data2


@task
def transform(data):
    """
    Transform and combine raw API data.

    No analytical calculations are performed here.
    """

    data1, data2 = data

    def convert_data_to_df(data):
        daily = data["daily"]

        number_of_days = len(daily["time"])

        latitude = [
            data["latitude"]
            for _ in range(number_of_days)
        ]

        longitude = [
            data["longitude"]
            for _ in range(number_of_days)
        ]

        df = pd.DataFrame({
            "latitude": latitude,
            "longitude": longitude,
            "date": daily["time"],

            # Temperature
            "temp_mean": daily["temperature_2m_mean"],
            "temp_max": daily["temperature_2m_max"],
            "temp_min": daily["temperature_2m_min"],

            # Precipitation
            "precipitation": daily["precipitation_sum"],
            "rain": daily["rain_sum"],
            "precipitation_hours": daily["precipitation_hours"],

            # Weather
            "weather_code": daily["weather_code"],

            # Sunshine
            "sunshine_duration": daily["sunshine_duration"],
            "daylight_duration": daily["daylight_duration"],

            # Humidity and cloud cover
            "humidity_mean": daily["relative_humidity_2m_mean"],
            "cloud_cover_mean": daily["cloud_cover_mean"],

            # Wind
            "wind_speed_mean": daily["wind_speed_10m_mean"],
            "wind_speed_max": daily["wind_speed_10m_max"],
            "wind_gusts_max": daily["wind_gusts_10m_max"],

            # Solar radiation
            "shortwave_radiation": daily["shortwave_radiation_sum"],
        })

        df["date"] = pd.to_datetime(df["date"])

        # Use Los Angeles time to classify the data
        current_date = pd.Timestamp.now(
            tz="America/Los_Angeles"
        ).date()

        df["data_type"] = df["date"].dt.date.apply(
            lambda date: (
                "forecast"
                if date >= current_date
                else "past_model_data"
            )
        )

        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        return df

    df1 = convert_data_to_df(data1)
    df2 = convert_data_to_df(data2)

    # Combine the two locations
    combined_df = pd.concat(
        [df1, df2],
        ignore_index=True
    )


    print(combined_df.head())
    print(combined_df.columns.tolist())

    return combined_df.to_dict(orient="records")


@task
def load(records):
    cursor = return_snowflake_conn()

    target_table = "LAB1.raw.weather_data_lab1"

    try:
        cursor.execute("BEGIN;")

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            latitude NUMBER(9, 6),
            longitude NUMBER(9, 6),
            date DATE,
            data_type VARCHAR,

            temp_mean FLOAT,
            temp_max FLOAT,
            temp_min FLOAT,

            precipitation FLOAT,
            rain FLOAT,
            precipitation_hours FLOAT,

            weather_code INTEGER,

            sunshine_duration FLOAT,
            daylight_duration FLOAT,

            humidity_mean FLOAT,
            cloud_cover_mean FLOAT,

            wind_speed_mean FLOAT,
            wind_speed_max FLOAT,
            wind_gusts_max FLOAT,

            shortwave_radiation FLOAT,

            PRIMARY KEY (latitude, longitude, date)
        );
        """)

        # Remove the existing forecast dataset
        cursor.execute(f"""
        DELETE FROM {target_table};
        """)

        insert_sql = f"""
        INSERT INTO {target_table} (
            latitude,
            longitude,
            date,
            data_type,

            temp_mean,
            temp_max,
            temp_min,

            precipitation,
            rain,
            precipitation_hours,

            weather_code,

            sunshine_duration,
            daylight_duration,

            humidity_mean,
            cloud_cover_mean,

            wind_speed_mean,
            wind_speed_max,
            wind_gusts_max,

            shortwave_radiation
        )
        VALUES (
            %s, %s, %s, %s,

            %s, %s, %s,

            %s, %s, %s,

            %s,

            %s, %s,

            %s, %s,

            %s, %s, %s,

            %s
        );
        """

        for record in records:
            print(
                record["latitude"],
                "-",
                record["longitude"],
                "-",
                record["date"],
                "-",
                record["data_type"]
            )

            cursor.execute(
                insert_sql,
                (
                    record["latitude"],
                    record["longitude"],
                    record["date"],
                    record["data_type"],

                    record["temp_mean"],
                    record["temp_max"],
                    record["temp_min"],

                    record["precipitation"],
                    record["rain"],
                    record["precipitation_hours"],

                    record["weather_code"],

                    record["sunshine_duration"],
                    record["daylight_duration"],

                    record["humidity_mean"],
                    record["cloud_cover_mean"],

                    record["wind_speed_mean"],
                    record["wind_speed_max"],
                    record["wind_gusts_max"],

                    record["shortwave_radiation"],
                )
            )

        cursor.execute("COMMIT;")

    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id="snowflake_connection_LAB1",
    start_date=datetime(2026, 9, 27),
    schedule=None,
    catchup=False,
) as dag:

    data = extract()
    records = transform(data)
    load_task = load(records)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_elt",
        wait_for_completion=True,
    )

    load_task >> trigger_dbt
