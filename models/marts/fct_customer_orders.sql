-- Import CTE source tables at the top
with customers as (
    select 
        * 
    from 
         {{ ref('stg_customers') }}   
),

orders as (
    select 
        *
    from 
        {{ ref('stg_orders') }}   
),

payments as (
    select 
        *
    from 
        {{ ref('stg_payments') }} 
),
-- Putting all the completed payments into this CTE
completed_payments as (
    select 
            order_id, 
            max(payment_date) as payment_finalized_date, 
            sum(amount) / 100.0 as total_amount_paid
    from 
        payments
    where payment_status <> 'fail'
    group by 1
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date as order_placed_at,
        orders.order_status,
        completed_payments.total_amount_paid,
        completed_payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name,
        -- Calculating the amount paid up to an order date by each customer
        sum(total_amount_paid) over (partition by orders.customer_id order by orders.order_date asc rows between unbounded preceding and current row) as rolling_paid_amount,
        rank() over (partition by orders.customer_id order by orders.order_date) as customer_payment_seq,
        case when (rank() over (partition by orders.customer_id order by orders.order_date) = 1)
            then 'yes'
        else 'no'
        end as is_first_payment,
        case when (rank() over (partition by orders.customer_id order by orders.order_date desc) = 1)
            then 'yes'
        else 'no'
        end as is_latest_payment,
        count(orders.order_id) over (partition by orders.customer_id order by orders.order_date asc rows between unbounded preceding and current row) as to_date_orders

    
    from 
         orders
    left join completed_payments on orders.order_id = completed_payments.order_id
    left join customers on orders.customer_id = customers.customer_id 
    ),

final as (    
 
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq
    from paid_orders 
    order by order_id
)

select * from final
