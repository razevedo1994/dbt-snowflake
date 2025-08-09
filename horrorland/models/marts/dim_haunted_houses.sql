select
    haunted_house_id
    , house_name
    , theme
    , fear_level
    , case 
        when fear_level >= 4 then 'Extreme'
        when fear_level >= 3 then 'High'
        when fear_level >= 2 then 'Medium'
        else 'Low'
    end as fear_category
    , case 
        when house_size_sqft >= 5000 then 'Large'
        when house_size_sqft >= 3000 then 'Medium'
        else 'Small'
    end as size_category
    , case 
        when duration_minutes < 15 then 'Short'
        when duration_minutes between 15 and 30 then 'Medium'
        when duration_minutes > 31 then 'Long'
    end as durarion_category
    , lower(house_status)
    , created_at
    , updated_at
from {{ ref('stg_attractions__haunted_houses') }}