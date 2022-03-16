with source as (
    select 
        * 
    from 
        {{ source('dbt-learning-project-343515','payments')}}
),

final as (
    select 
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        amount,
        created as payment_date
    from 
        source    
)

select * from final