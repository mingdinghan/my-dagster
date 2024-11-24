{{
    config(
        materialized="table"
    )
}}

select
    category_id,
    name,
    last_update
from {{ source('dvd_rental', 'category') }}
