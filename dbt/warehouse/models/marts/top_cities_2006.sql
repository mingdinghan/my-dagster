{{
    config(
        materialized="table"
    )
}}

select
    sum(payment.amount) as revenue,
    city.city,
    country.country
from {{ ref('payment') }} as payment
inner join {{ ref('rental') }} as rental on payment.rental_id = rental.rental_id
inner join {{ ref('customer') }} as customer on customer.customer_id = rental.customer_id
inner join {{ ref('address') }} as address on address.address_id = customer.address_id
inner join {{ ref('city') }} as city on city.city_id = address.city_id
inner join {{ ref('country') }} as country on country.country_id = city.country_id
where year(rental.rental_date) = 2006
group by city.city, country.country
order by revenue desc
limit 10
