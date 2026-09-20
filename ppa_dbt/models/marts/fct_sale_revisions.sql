{{
    config(
        materialized='incremental',
        unique_key='sale_revision_id',
        incremental_strategy='merge'
    )
}}

with sale_revisions as (
    select
    sale_revision_id,
    sale_id,
    sale_revision_number,
    sale_revision_reason,
    sale_revision_subtotal,
    sale_revision_discount,
    sale_revision_total,
    sale_revision_amount_tendered,
    sale_revision_change,
    payment_method_id,
    payment_method_name_snapshot,
    created_at,
    device_id,
    round(coalesce((sale_revision_discount / nullif(sale_revision_subtotal, 0.0))* 100, 0.0), 1) as sale_discount_pct
    from {{ref('stg_sale_revisions')}}

    {% if is_incremental() %}
        where created_at >= (select max(created_at) - interval '2 days' from {{ this }})
    {% endif %}
)

select * from sale_revisions