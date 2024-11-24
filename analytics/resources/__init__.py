from dagster import ConfigurableResource

class PostgresqlDatabaseResource(ConfigurableResource):
    host_name: str
    database_name: str
    username: str
    password: str
    port: str
    