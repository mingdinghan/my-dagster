{{
    config(
        materialized="table",
        incremental_strategy="append",
    )
}}

select
    payment_id,
    rental_id,
    customer_id,
    staff_id,
    amount,
    payment_date
from {{ source('dvd_rental', 'payment') }}
order by payment_date
{% if is_incremental() %}
    where payment_date > (select max(payment_date) from {{ this }} )
{% endif %}
