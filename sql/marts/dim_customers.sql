-- Tabela de dimensão de clientes
-- Agrega informações de clientes com métricas de pedidos

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('fact_orders') }}
),

customer_orders as (
    select
        customer_id,
        count(order_id) as total_orders,
        sum(total_amount) as total_spent,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.city,
        c.state,
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.total_spent, 0) as total_spent,
        co.first_order_date,
        co.last_order_date,
        case
            when co.total_orders > 5 then 'VIP'
            when co.total_orders > 1 then 'Recorrente'
            else 'Novo'
        end as customer_segment
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from final
