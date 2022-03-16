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
-- Putting all the completed payments into this CTE
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
        customers.first_name    as customer_first_name,
        customers.last_name as customer_last_name,
        -- Calculating the amount paid up to an order date by each customer
        sum(total_amount_paid) over (partition by orders.user_id order by orders.order_date asc rows between unbounded preceding and current row) as to_date_paid_amount
    from 
         orders
    left join completed_payments on orders.id = completed_payments.order_id
    left join customers on orders.user_id = customers.id 
    ),
-- first, last, and count of orders by each customer:
customer_orders as (
    select 
        customers.id as customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders
    from 
        customers 
    left join orders on orders.user_id = customers.id 
    group by 1
    ),

final as (    
 
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
        case 
            when customer_orders.first_order_date = paid_orders.order_placed_at
        then 'new'
        else 'return' 
        end as nvsr,
        customer_orders.first_order_date as fdos
    from paid_orders 
    left join customer_orders using (customer_id)
    order by order_id
)

select * from final
