-- Tabela de dimensão de produtos
-- Agrega informações de produtos com métricas de vendas

with products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select * from {{ source('raw', 'order_items') }}
),

product_sales as (
    select
        product_id,
        count(*) as times_sold,
        sum(quantity) as total_quantity_sold,
        sum(quantity * unit_price) as total_revenue
    from order_items
    group by product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        p.stock_quantity,
        coalesce(ps.times_sold, 0) as times_sold,
        coalesce(ps.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(ps.total_revenue, 0) as total_revenue
    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from final
