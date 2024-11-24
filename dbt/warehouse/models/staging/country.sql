{{
    config(
        materialized="table"
    )
}}

select
    country_id,
    country,
    last_update
from {{ source('dvd_rental', 'country') }}
