-- Modelo de estagiação de pedidos
-- Realiza a ingestão e limpeza dos dados brutos de pedidos

with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        id as order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        shipping_address,
        created_at,
        updated_at
    from source
)

select * from renamed
