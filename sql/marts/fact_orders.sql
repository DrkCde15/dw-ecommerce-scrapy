-- Tabela fato de pedidos
-- Armazena as transações de pedidos com métricas

with order_items as (
    select * from {{ ref('int_order_items') }}
),

final as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        status,
        quantity,
        unit_price,
        total_price,
        created_at
    from order_items
)

select * from final
