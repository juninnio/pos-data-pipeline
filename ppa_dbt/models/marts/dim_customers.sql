with customers as (
    select
        customer_id,
        customer_name,
        customer_phone,
        customer_address,
        created_at,
        updated_at,
        deleted_at
    from {{ref('stg_customers')}}
)

select * from customers