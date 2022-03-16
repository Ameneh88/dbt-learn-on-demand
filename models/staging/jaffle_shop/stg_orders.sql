with source as (
    select 
        * 
    from 
        {{ source('dbt-learning-project-343515', 'orders') }}
), 

final as (
    select 
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status
    from 
        source    
)

select * from final