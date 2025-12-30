with base as (
    select * from {{ ref('flow') }}
)

select * from base
