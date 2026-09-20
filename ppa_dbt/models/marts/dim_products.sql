with products as (
    select 
        product_id, 
        product_name,
        category_id,
        cost, price,
        created_at,
        updated_at,
        deleted_at,
        (price - cost) as unit_margin from {{ref('stg_products')}}
)

select * from products