-- Tabela de dimensão unificada de produtos
-- Combina dados de Americanas, KaBuM e Amazon em uma unica tabela

with americanas as (
    select
        product_id,
        product_name,
        brand,
        price,
        list_price as original_price,
        category,
        seller,
        image_url,
        url,
        source,
        scraped_at,
        case
            when price < 100 then 'Economico'
            when price < 500 then 'Medio'
            when price < 2000 then 'Premium'
            else 'Luxo'
        end as price_tier,
        case when available_quantity > 0 then true else false end as in_stock
    from {{ ref('stg_americanas') }}
),

kabum as (
    select
        product_id,
        product_name,
        brand,
        price,
        old_price as original_price,
        category,
        seller,
        image_url,
        url,
        source,
        scraped_at,
        case
            when price < 100 then 'Economico'
            when price < 500 then 'Medio'
            when price < 2000 then 'Premium'
            else 'Luxo'
        end as price_tier,
        case when stock > 0 then true else false end as in_stock
    from {{ ref('stg_kabum') }}
),

amazon as (
    select
        product_id,
        product_name,
        null as brand,
        price,
        original_price,
        null as category,
        null as seller,
        image_url,
        url,
        source,
        scraped_at,
        case
            when price < 100 then 'Economico'
            when price < 500 then 'Medio'
            when price < 2000 then 'Premium'
            else 'Luxo'
        end as price_tier,
        true as in_stock
    from {{ ref('stg_amazon') }}
),

all_products as (
    select * from americanas
    union all
    select * from kabum
    union all
    select * from amazon
),

final as (
    select
        md5(concat(product_id, '|', source)) as id,
        product_id,
        product_name,
        brand,
        price,
        original_price,
        case
            when original_price > 0 and original_price > price then
                round(((original_price - price) / original_price * 100)::numeric, 2)
            else 0
        end as discount_percentage,
        category,
        seller,
        image_url,
        url,
        source,
        price_tier,
        in_stock,
        scraped_at,
        row_number() over (partition by product_id, source order by scraped_at desc) as rn
    from all_products
    where product_name is not null
      and price > 0
)

select
    id,
    product_id,
    product_name,
    brand,
    price,
    original_price,
    discount_percentage,
    category,
    seller,
    image_url,
    url,
    source,
    price_tier,
    in_stock,
    scraped_at
from final
where rn = 1
