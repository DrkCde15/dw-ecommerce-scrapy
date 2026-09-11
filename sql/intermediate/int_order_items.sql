-- Modelo intermediário de itens de pedido
-- Realiza o join entre pedidos e seus itens

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ source('raw', 'order_items') }}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.quantity * oi.unit_price as total_price,
        o.created_at
    from orders o
    inner join order_items oi on o.order_id = oi.order_id
)

select * from joined
