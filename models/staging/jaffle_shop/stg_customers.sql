with source as (
    select * from {{ source('dbt-learning-project-343515','customers') }}
),

final as (
    select 
        id as customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from 
        source     
)

select * from final