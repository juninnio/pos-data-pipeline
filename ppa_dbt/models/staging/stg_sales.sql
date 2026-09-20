with standardized as (
    select
    id as sale_id,
    receipt_number as sale_receipt_number,
    device_id,
    customer_id,
    CAST(customer_name_snapshot as varchar(255)) as sale_customer_name_snapshot,
    lower(trim(status)) as sale_status,
    current_revision_id,
    created_at,
    updated_at,
    completed_at,
    refunded_at
    from {{source('ppa_dbt','sales')}}
)

select * from standardized