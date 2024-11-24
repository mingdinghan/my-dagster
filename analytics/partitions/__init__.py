import datetime
from dagster import DailyPartitionsDefinition

weather_daily_partition = DailyPartitionsDefinition(start_date=datetime.datetime(2024, 3, 4))