{{
    config(
        materialized="table"
    )
}}

select
    store_id,
    address_id,
    manager_staff_id,
    last_update
from {{ source('dvd_rental', 'store') }}
