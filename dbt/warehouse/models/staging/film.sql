{{
    config(
        materialized="table"
    )
}}

select
    film_id,
    title,
    description,
    release_year,
    replacement_cost,
    length,
    rating,
    rental_rate,
    rental_duration,
    special_features,
    language_id,
    fulltext,
    last_update
from {{ source('dvd_rental', 'film') }}
