-- Import CTE source tables at the top
with customers as (
    select 
        * 
    from 
         {{ source('dbt-learning-project-343515','customers') }}   
),

orders as (
    select 
        *
    from 
        {{ source('dbt-learning-project-343515','orders') }}   
),

payments as (
    select 
        *
    from 
        {{ source('dbt-learning-project-343515','payments') }} 
),

completed_payments as (
    select 
            orderid as order_id, 
            max(created) as payment_finalized_date, 
            sum(amount) / 100.0 as total_amount_paid
    from 
        payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select 
        orders.id as order_id,
        orders.user_id    as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        completed_payments.total_amount_paid,
        completed_payments.payment_finalized_date,
        c.first_name    as customer_first_name,
        c.last_name as customer_last_name,
        sum(total_amount_paid) over (partition by orders.user_id order by orders.order_date asc rows between unbounded preceding and current row) as to_date_paid_amount
    from 
         orders
    left join completed_payments on orders.id = completed_payments.order_id
    left join customers as c on orders.user_id = c.id 
    ),

customer_orders as (
    select 
        c.id as customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders
    from 
        customers as c 
    left join orders on orders.user_id = c.id 
    group by 1
    ),
final as (    
 
    select
        p.*,
        row_number() over (order by p.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
        case 
            when c.first_order_date = p.order_placed_at
        then 'new'
        else 'return' 
        end as nvsr,
        c.first_order_date as fdos
    from paid_orders p
    left join customer_orders as c using (customer_id)
    order by order_id
)

select * from final
