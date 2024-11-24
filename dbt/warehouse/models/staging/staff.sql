{{
    config(
        materialized="table"
    )
}}

select
    staff_id,
    first_name,
    last_name,
    username,
    email,
    address_id,
    store_id,
    password,
    last_update,
    active
from {{ source('dvd_rental', 'staff') }}
