{{
    config(
        materialized="table",
        unique_key=["inventory_id"],
        incremental_strategy="delete+insert",
    )
}}

select
    film_id,
    inventory_id,
    store_id,
    last_update
from {{ source('dvd_rental', 'inventory') }}
{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} ) - interval '48 hours'
{% endif %}
