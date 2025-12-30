with base as (
    select * from {{ ref('campaign') }}
)

select * from base
