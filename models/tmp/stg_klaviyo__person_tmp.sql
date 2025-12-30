with base as (
    select * from {{ ref('person') }}
)

select * from base
