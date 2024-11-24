{{
    config(
        materialized="table"
    )
}}

select
    sum(payment.amount) as revenue,
    address.address
from {{ ref('payment') }} as payment
inner join {{ ref('rental') }} as rental on payment.rental_id = rental.rental_id
inner join {{ ref('staff') }} as staff on staff.staff_id = payment.staff_id
inner join {{ ref('store') }} as store on store.store_id = staff.store_id
inner join {{ ref('address') }} as address on address.address_id = store.address_id
where year(rental.rental_date) = 2006
group by address.address
order by revenue desc
