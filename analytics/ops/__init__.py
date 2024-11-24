from sqlalchemy import Table, MetaData, URL, create_engine
from sqlalchemy.dialects import postgresql
from dagster import op, OpExecutionContext


from analytics.resources import PostgresqlDatabaseResource


def upsert_to_database(
        context: OpExecutionContext,
        postgres_conn: PostgresqlDatabaseResource,
        data: list[dict],
        table: Table,
        metadata: MetaData
    ) -> None:
    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=postgres_conn.username,
        password=postgres_conn.password,
        host=postgres_conn.host_name,
        port=postgres_conn.port,
        database=postgres_conn.database_name,
    )
    engine = create_engine(connection_url)
    context.log.info("Creating database table if not exist")
    metadata.create_all(engine)
    key_columns = [
        pk_column.name for pk_column in table.primary_key.columns.values()
    ]
    insert_statement = postgresql.insert(table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={
            c.key: c for c in insert_statement.excluded if c.key not in key_columns
        },
    )
    context.log.info("Upserting records to database table")
    with engine.begin() as connection:
        try:
            result = connection.execute(upsert_statement)
        except Exception as e:
            raise Exception(f"Failed to upsert to database, {e}")
    context.log.info(f"Completed upsert to database table. Rows affected: {result.rowcount}")
