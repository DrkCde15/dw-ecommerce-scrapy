-- Modelo de estagiação de produtos
-- Realiza a ingestão e limpeza dos dados brutos de produtos

with source as (
    select * from {{ source('raw', 'products') }}
),

renamed as (
    select
        id as product_id,
        name as product_name,
        category,
        price,
        stock_quantity,
        created_at
    from source
)

select * from renamed
