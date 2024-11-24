import csv
import requests
import datetime
from dagster import (
    Config,
    EnvVar,
    OpExecutionContext,
    asset,
    AutoMaterializePolicy,
    FreshnessPolicy,
)
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, URL, create_engine, text
import pandas as pd

from analytics.resources import PostgresqlDatabaseResource
from analytics.ops import upsert_to_database
from analytics.partitions import weather_daily_partition

class CitiesConfig(Config):
    city_path: str = "analytics/data/australian_capital_cities.csv"

@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def cities(context: OpExecutionContext, config: CitiesConfig) -> list[dict]:
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

@asset(
    partitions_def=weather_daily_partition,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def raw_weather(context: OpExecutionContext, config: WeatherApiConfig, cities: list[dict]) -> list[dict]:
    context.log.info(f"Iterating through list of cities: {cities}")
    city_weather = []
    for city in cities:
        dt = int(datetime.datetime.strptime(context.partition_key, "%Y-%m-%d").timestamp())
        context.log.info(f"Getting weather data for: {city}, for partition {context.partition_key}")
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


@asset(
    partitions_def=weather_daily_partition,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def transformed_weather(context: OpExecutionContext, raw_weather: list[dict]) -> list[dict]:
    context.log.info("Transforming data")
    df = pd.json_normalize(raw_weather, record_path="data", meta="name")
    df_renamed = df.rename(columns={"dt": "datetime", "temp": "temperature"})
    df_selected = df_renamed[["name", "datetime", "temperature"]]
    return df_selected.to_dict(orient="records")


@asset(
    partitions_def=weather_daily_partition,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def metric_weather_table(context: OpExecutionContext, postgres_conn: PostgresqlDatabaseResource, transformed_weather: list[dict]) -> None:
    metadata = MetaData()
    table = Table(
        "weather_historical",
        metadata,
        Column("name", String, primary_key=True),
        Column("datetime", Integer, primary_key=True),
        Column("temperature", Float),
    )
    upsert_to_database(context=context, postgres_conn=postgres_conn, data=transformed_weather, table=table, metadata=metadata)


@asset(
    deps=[metric_weather_table],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=1,
        cron_schedule="0 0 * * *"
    ),
)
def farenheit_weather_table(context: OpExecutionContext, postgres_conn: PostgresqlDatabaseResource) -> None:
    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=postgres_conn.username,
        password=postgres_conn.password,
        host=postgres_conn.host_name,
        port=postgres_conn.port,
        database=postgres_conn.database_name,
    )
    engine = create_engine(connection_url)
    with engine.begin() as connection:
        try:
            context.log.info("Dropping table")
            result = connection.execute(text("drop table if exists weather_farenheit"))
            context.log.info("Executing CTAS")
            result = connection.execute(text("""create table weather_farenheit as
                select
                    name,
                    datetime,
                    ((temperature  * 9/5) + 32) as temperature
                from weather_historical
            """))
            context.log.info(f"Completed CTAS. Rows affected: {result.rowcount}")
        except Exception as e:
            raise Exception(f"Failed to transform from data, {e}")
