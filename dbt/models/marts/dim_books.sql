-- Tabela de dimensão de livros
-- Agrega informações de livros raspados com métricas e classificações

with books as (
    select * from {{ ref('stg_books') }}
),

book_stats as (
    select
        category,
        count(*) as total_books,
        avg(price) as avg_price,
        min(price) as min_price,
        max(price) as max_price,
        avg(rating) as avg_rating
    from books
    group by category
),

final as (
    select
        b.book_id,
        b.product_id,
        b.book_name,
        b.price,
        b.rating,
        case
            when b.rating >= 4 then 'Excelente'
            when b.rating >= 3 then 'Bom'
            when b.rating >= 2 then 'Regular'
            else 'Ruim'
        end as rating_label,
        b.availability,
        b.image_url,
        b.url,
        b.category,
        b.source,
        b.scraped_at,
        bs.total_books as category_total_books,
        round(bs.avg_price, 2) as category_avg_price,
        round(bs.avg_rating, 2) as category_avg_rating,
        case
            when b.price < bs.avg_price * 0.8 then 'Abaixo do preco medio'
            when b.price > bs.avg_price * 1.2 then 'Acima do preco medio'
            else 'Preco medio'
        end as price_vs_category
    from books b
    left join book_stats bs on b.category = bs.category
)

select * from final
