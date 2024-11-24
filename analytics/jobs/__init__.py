
from dagster import job, daily_partitioned_config
from datetime import datetime
from analytics.ops.weather import get_cities, extract_weather, transform_weather, load_weather_to_database

@daily_partitioned_config(start_date=datetime(2024, 1, 1))
def weather_etl_daily_partition(start: datetime, _end: datetime):
    return {
        "ops": {
            "extract_weather": {
                "config": {
                    "date": start.strftime("%Y-%m-%d")
                }
            }
        }
    }

@job(config=weather_etl_daily_partition)
def run_weather_etl():
    weather_data = extract_weather(get_cities())
    load_weather_to_database(transform_weather(weather_data))
