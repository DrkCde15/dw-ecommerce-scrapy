-- Modelo de estagiação de clientes
-- Realiza a ingestão e limpeza dos dados brutos de clientes

with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        id as customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        state,
        created_at
    from source
)

select * from renamed
