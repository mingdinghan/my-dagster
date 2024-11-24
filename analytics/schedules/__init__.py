from dagster import ScheduleDefinition, build_schedule_from_partitioned_job
from analytics.jobs import run_weather_etl

# weather_etl_schedule = ScheduleDefinition(
#     job=run_weather_etl,
#     cron_schedule="* * * * 1-5"
# )
weather_etl_schedule = build_schedule_from_partitioned_job(job=run_weather_etl)