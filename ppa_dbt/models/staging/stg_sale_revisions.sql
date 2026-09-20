with standardized as (
    select
    id as sale_revision_id,
    sale_id,
    cast(revision_number as int) as sale_revision_number,
    lower(trim(reason)) as sale_revision_reason,
    cast(subtotal_minor as int) as sale_revision_subtotal,
    cast(discount_minor as int) as sale_revision_discount,
    cast(total_minor as int) as sale_revision_total,
    cast(amount_tendered_minor as int) as sale_revision_amount_tendered,
    cast(change_minor as int) as sale_revision_change,
    payment_method_id,
    payment_method_name_snapshot,
    created_at,
    created_by_device_id as device_id
    from {{source('ppa_dbt', 'sale_revisions')}}
)

select * from standardized