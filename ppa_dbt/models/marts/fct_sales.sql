{{
    config(
        materialized='incremental',
        unique_key='sale_id',
        incremental_strategy='merge'
    )
}}

with sale as (
    select
    sale_id,
    sale_receipt_number,
    device_id,
    customer_id,
    sale_customer_name_snapshot,
    sale_status,
    current_revision_id,
    created_at,
    updated_at,
    completed_at,
    refunded_at,
    CASE 
        when sale_status = 'refunded' then true
        else false
    END as is_fully_refunded
    FROM {{ref('stg_sales')}}

    {% if is_incremental() %}
        where updated_at >= (
        select coalesce(
            max(updated_at),
            cast('1900-01-01' as timestamptz)
        ) - interval '2 days'
        from {{ this }}
    )
    {% endif %}
)

select * from sale