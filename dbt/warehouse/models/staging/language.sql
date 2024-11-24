{{
    config(
        materialized="table"
    )
}}

select
    language_id,
    name,
    last_update
from {{ source('dvd_rental', 'language') }}
