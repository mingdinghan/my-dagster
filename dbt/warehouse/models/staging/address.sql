{{
    config(
        materialized="incremental",
        unique_key=["address_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    address_id,
    address,
    phone,
    district,
    postal_code,
    city_id,
    last_update
from {{ source('dvd_rental', 'address') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
