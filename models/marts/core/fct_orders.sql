with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
order_payments as (
    select
        order_id,
        sum(payment_amount) as order_amount
    from payments
    where payment_status = 'success'
    group by order_id
),
final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        coalesce(order_payments.order_amount, 0) as order_amount
    from orders
    left join order_payments
        using (order_id)
)
select * from final
