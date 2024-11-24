{{
    config(
        materialized="table"
    )
}}

select
    customer_id,
    first_name,
    last_name,
    create_date,
    active,
    address_id,
    email,
    store_id,
    last_update
from {{ source('dvd_rental', 'customer') }}
