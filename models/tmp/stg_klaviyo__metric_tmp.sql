with base as (
    select * from {{ ref('metric') }}
)

select * from base
