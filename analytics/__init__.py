from dagster import Definitions, EnvVar, load_assets_from_package_module

from analytics.jobs import run_weather_etl
from analytics.resources import PostgresqlDatabaseResource
from analytics.schedules import weather_etl_schedule
from analytics.assets import weather
from analytics.assets.airbyte.airbyte import airbyte_assets
from analytics.assets.dbt.dbt import dbt_warehouse, dbt_warehouse_resource


weather_assets = load_assets_from_package_module(weather, group_name="weather")

defs = Definitions(
    assets=[*weather_assets, airbyte_assets, dbt_warehouse],
    jobs=[run_weather_etl],
    schedules=[weather_etl_schedule],
    resources={
        "postgres_conn": PostgresqlDatabaseResource(
            host_name=EnvVar("postgres_host_name"),
            database_name=EnvVar("postgres_database_name"),
            username=EnvVar("postgres_username"),
            password=EnvVar("postgres_password"),
            port=EnvVar("postgres_port")
        ),
        "dbt_warehouse_resource": dbt_warehouse_resource
    }
)