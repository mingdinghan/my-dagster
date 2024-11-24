{{
    config(
        materialized="table"
    )
}}

select
    film_id,
    actor_id,
    last_update
from {{ source('dvd_rental', 'film_actor') }}
