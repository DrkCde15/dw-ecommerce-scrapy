-- Modelo de estagiação de livros
-- Realiza a ingestão e limpeza dos dados brutos raspados via Scrapy

with source as (
    select * from {{ source('raw', 'books') }}
),

renamed as (
    select
        book_id,
        product_id,
        trim(name) as book_name,
        price,
        rating::int as rating,
        trim(availability) as availability,
        image_url,
        url,
        category,
        source,
        scraped_at
    from source
    where price > 0
)

select * from renamed
