-- Modelo de estagiação de produtos Amazon
-- Realiza a ingestão e limpeza dos dados brutos raspados via HTML

with source as (
    select * from {{ source('raw', 'amazon') }}
),

renamed as (
    select
        id,
        product_id,
        trim(name) as product_name,
        price,
        original_price,
        rating,
        reviews_count,
        image_url,
        url,
        trim(source) as source,
        scraped_at
    from source
    where price > 0
      and product_id is not null
      and name is not null
)

select * from renamed
