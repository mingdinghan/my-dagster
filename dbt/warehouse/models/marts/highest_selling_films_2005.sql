{{
    config(
        materialized="table"
    )
}}

select
    sum(payment.amount) as revenue,
    film.title
from {{ ref('payment') }} as payment
inner join {{ ref('rental') }} as rental on payment.rental_id = rental.rental_id
inner join {{ ref('inventory') }} as inventory on inventory.inventory_id = rental.inventory_id
inner join {{ ref('film') }} as film on film.film_id = inventory.inventory_id
where year(rental.rental_date) = 2005
group by film.title
order by revenue desc
limit 10
