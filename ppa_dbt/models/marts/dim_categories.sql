with categories as (
    select
        category_id,
        category_name,
        created_at,
        updated_at,
        deleted_at
    from {{ref('stg_categories')}}
)

select * from categories