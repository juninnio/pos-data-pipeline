with standardized as (
    select
        id as payment_method_id,
        lower(trim(name)) as payment_method_name,
        created_at,
        updated_at,
        deleted_at
    from {{source('ppa_dbt','payment_methods')}}
)

select * from standardized