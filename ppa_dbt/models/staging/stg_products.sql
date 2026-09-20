with renamed as (
    select
        CAST(id as text) as product_id,
        CAST(name as text) as product_name,
        category_id,
        CAST(cost_minor as int) as cost,
        CAST(price_minor as int) as price,
        created_at,
        updated_at,
        deleted_at
    from {{source('ppa_dbt', 'products')}}
)

select * from renamed