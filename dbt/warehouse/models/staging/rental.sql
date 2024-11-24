{{
    config(
        materialized="incremental",
        unique_key=["rental_id"],
        incremental_strategy="delete+insert",
    )
}}

select
    rental_id,
    customer_id,
    staff_id,
    inventory_id,
    rental_date,
    return_date,
    last_update
from {{ source('dvd_rental', 'rental') }}
{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} ) - interval '48 hours'
{% endif %}
order by rental_date
