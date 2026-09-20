with standardized as (
    select
        id as customer_id,
        lower(trim(name)) as customer_name,
        cast(phone as varchar(255)) as customer_phone,
        lower(trim(address)) as customer_address,
        created_at,
        updated_at,
        deleted_at
    from {{source('ppa_dbt', 'customers')}}
)

select * from standardized