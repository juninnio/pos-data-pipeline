with renamed as (
    select
        id as category_id,
        CAST(name as text) as category_name,
        created_at,
        updated_at,
        deleted_at
    from {{source('ppa_dbt', 'categories')}}
)

select * from renamed