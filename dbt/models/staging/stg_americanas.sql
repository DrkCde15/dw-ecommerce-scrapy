-- Modelo de estagiação de produtos Americanas
-- Realiza a ingestão e limpeza dos dados brutos da VTEX API

with source as (
    select * from {{ source('raw', 'americanas') }}
),

renamed as (
    select
        id,
        product_id,
        trim(name) as product_name,
        trim(brand) as brand,
        price,
        list_price,
        available_quantity,
        trim(category) as category,
        image_url,
        url,
        trim(seller) as seller,
        trim(source) as source,
        scraped_at
    from source
    where price > 0
      and product_id is not null
      and name is not null
)

select * from renamed
