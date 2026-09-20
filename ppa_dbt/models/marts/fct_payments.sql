{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}

with payments as (
    select
        p.payment_id,
        p.sale_id,
        p.sale_revision_id,
        p.payment_method_id,
        p.payment_method_name_snapshot,
        p.payment_amount_tendered,
        p.payment_change,
        p.payment_amount_tendered - p.payment_change as payment_amount_net,
        s.sale_revision_total as sale_total,
        p.created_at
    from {{ref('stg_payments')}} p
    inner join
    {{ref('stg_sale_revisions')}} s
    on 
    p.sale_revision_id = s.sale_revision_id
    {% if is_incremental() %}
        where p.created_at >= (
        select coalesce(
            max(created_at),
            cast('1900-01-01' as timestamptz)
        ) - interval '2 days'
        from {{ this }})
    {% endif %}
)

select * from payments