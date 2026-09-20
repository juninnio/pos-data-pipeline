{{
    config(
        materialized='incremental',
        unique_key='item_id',
        incremental_strategy='merge'
    )
}}
with revision_items as (
    select item_id,
    s.sale_id,
    i.sale_revision_id as sale_revision_id,
    s.sale_revision_number as sale_revision_number,
    product_id,
    product_name_snapshot,
    category_name_snapshot,
    item_quantity,
    item_unit_cost,
    item_unit_price,
    item_line_total,
    s.created_at as sale_revision_created_at
    from {{ref('stg_sale_revision_items')}} i
    inner join
    {{ref('stg_sale_revisions')}} s
    on
    i.sale_revision_id = s.sale_revision_id

    {% if is_incremental() %}
        where sale_revision_created_at >= (
        select coalesce(
            max(sale_revision_created_at),
            cast('1900-01-01' as timestamptz)
        ) - interval '2 days'
        from {{ this }}
    )
    {% endif %}
)

select * from revision_items