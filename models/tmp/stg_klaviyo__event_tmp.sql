with base as (
    select * from {{ ref('event') }}
)

select * from base
