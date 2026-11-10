with source as (
    select * from {{ source('raw', 'sellers') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by seller_id order by updated_at desc, _loaded_at desc) = 1
)

select
    seller_id,
    trim(seller_name) as seller_name,
    trim(city) as city,
    upper(state) as state_code,
    cast(rating as decimal(2, 1)) as rating,
    created_at,
    updated_at
from deduplicated
