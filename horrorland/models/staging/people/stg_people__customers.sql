select 
    customer_id
    , age
    , gender
    , email
    , registration_date
    , is_vip_member
    , preferred_scare_level
from {{ source('PEOPLE', 'CUSTOMERS') }}