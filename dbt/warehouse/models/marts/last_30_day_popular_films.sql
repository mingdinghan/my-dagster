{{
    config(
        materialized="table"
    )
}}

-- get the most popular films in the last 30 days

with filtered_rental as (
    select *, max(rental_date) over (order by false) as max_date
    from {{ ref('rental') }}
    qualify rental_date > max_date - interval '30 days'
)

select
    film.title,
    count(*) as number_of_rentals
from filtered_rental
inner join {{ ref('film') }} as film
group by
    film.title
order by number_of_rentals desc
limit 10
