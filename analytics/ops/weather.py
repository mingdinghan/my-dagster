import csv
import requests
from dagster import op, Config, EnvVar, OpExecutionContext
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
import pandas as pd
import datetime

from analytics.resources import PostgresqlDatabaseResource
from analytics.ops import upsert_to_database

class CitiesConfig(Config):
    city_path: str = "analytics/data/australian_capital_cities.csv"

@op
def get_cities(context: OpExecutionContext, config: CitiesConfig) -> list[dict]:
    context.log.info("Opening cities file")
    cities = []
    with open(config.city_path, "r") as fp:
        context.log.info("Reading cities data")
        csv_reader = csv.reader(fp)
        header = next(csv_reader) # skip first row
        for row in csv_reader:
            cities.append({"name": row[0], "lat": row[1], "lon": row[2]})
    context.log.info("Returning cities data")
    return cities


class WeatherApiConfig(Config):
    api_key: str = EnvVar("weather_api_key")
    temperature_unit: str = "metric"
    date: str


@op
def extract_weather(context: OpExecutionContext, config: WeatherApiConfig, cities: list[dict]) -> list[dict]:
    context.log.info(f"Iterating through list of cities: {cities}")
    city_weather = []
    for city in cities:
        dt = int(datetime.datetime.strptime(config.date, "%Y-%m-%d").timestamp())
        context.log.info(f"Getting weather data for: {city}, for partition {config.date}")
        params = {
            "lat": city.get("lat"),
            "lon": city.get("lon"),
            "units": config.temperature_unit,
            "appid": config.api_key,
            "dt": dt,
        }
        response = requests.get("https://api.openweathermap.org/data/3.0/onecall/timemachine", params=params)
        if response.status_code == 200:
            data = response.json()
            data["name"] = city.get("name")
            city_weather.append(data)
        else:
            raise Exception(
                f"Failed to extract data from Open Weather API. Status Code: {response.status_code}. Response: {response.text}"
            )
    context.log.info(f"Returning weather data for cities: {cities}")
    return city_weather


@op
def transform_weather(context: OpExecutionContext, data: list[dict]) -> list[dict]:
    context.log.info("Transforming data")
    df = pd.json_normalize(data, record_path="data", meta="name")
    df_renamed = df.rename(columns={"dt": "datetime", "temp": "temperature"})
    df_selected = df_renamed[["name", "datetime", "temperature"]]
    return df_selected.to_dict(orient="records")

@op
def load_weather_to_database(context: OpExecutionContext, postgres_conn: PostgresqlDatabaseResource, data: list[dict]) -> None:
    metadata = MetaData()
    table = Table(
        "weather_historical",
        metadata,
        Column("name", String, primary_key=True),
        Column("datetime", Integer, primary_key=True),
        Column("temperature", Float),
    )
    upsert_to_database(context=context, postgres_conn=postgres_conn, data=data, table=table, metadata=metadata)
