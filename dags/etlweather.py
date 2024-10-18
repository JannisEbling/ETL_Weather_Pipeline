import json

import requests
from airflow import DAG
from airflow.decorators import task

# For every datasource there are hooks available in airflow
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Latitude and longitude for Düsseldorf
LATITUDE = "51.2277"
LONGITUDE = "6.7762"
POSTGRES_CONN_ID = "postgres_ID"
API_CONN_ID = "api_ID"

default_args = {"owner": "airflow", "start_date": days_ago(1)}

# Directed Acyclic Graph
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dags:

    @task()
    def extract_weather_data():
        # Create HTTP Hook
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")

        # Build the API endpoint, together with the Hook it requests data from the following page:
        # https://api.open-meteo.com/v1/forecast?latitude=51.2277&longitude=6.7762&current_weather=true
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"

        # Get Data with the Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data["current_weather"]
        transformed_data = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current_weather["temperature"],
            "windspeed": current_weather["windspeed"],
            "winddirection": current_weather["winddirection"],
            "weathercode": current_weather["weathercode"],
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )

        # Insert transformed data into the table
        cursor.execute(
            """
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
            (
                transformed_data["latitude"],
                transformed_data["longitude"],
                transformed_data["temperature"],
                transformed_data["windspeed"],
                transformed_data["winddirection"],
                transformed_data["weathercode"],
            ),
        )

        conn.commit()
        cursor.close()

    # ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
