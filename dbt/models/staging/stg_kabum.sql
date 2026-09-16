-- Modelo de estagiação de produtos KaBuM
-- Realiza a ingestão e limpeza dos dados brutos da API interna

with source as (
    select * from {{ source('raw', 'kabum') }}
),

renamed as (
    select
        id,
        product_id,
        trim(name) as product_name,
        trim(brand) as brand,
        price,
        old_price,
        discount_percentage,
        stock,
        trim(category) as category,
        image_url,
        url,
        trim(seller) as seller,
        rating,
        reviews_count,
        trim(source) as source,
        scraped_at
    from source
    where price > 0
      and product_id is not null
      and name is not null
)

select * from renamed
