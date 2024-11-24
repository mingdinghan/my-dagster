from dagster import EnvVar, AutoMaterializePolicy, FreshnessPolicy
from dagster_airbyte import AirbyteResource, load_assets_from_airbyte_instance

airbyte_resource = AirbyteResource(
    host="localhost",
    port="8000",
    username="airbyte",
    password="password",
)

airbyte_assets = load_assets_from_airbyte_instance(
    airbyte_resource,
    # This line is custom if you have multiple airbyte connections
    connection_filter=lambda meta: "dvd_rental_snowflake" in meta.name,
    key_prefix="dvd_rental",
    connection_to_freshness_policy_fn=lambda _: FreshnessPolicy(maximum_lag_minutes=5),
    connection_to_auto_materialize_policy_fn=lambda _: AutoMaterializePolicy.eager(),
)
