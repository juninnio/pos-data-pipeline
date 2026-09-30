{{ config(materialized='table') }}

with sales as (
    select
        sale_id,
        current_revision_id
    from {{ ref('stg_sales') }}
),

revisions as (
    select
        sale_revision_id,
        sale_id,
        sale_revision_number,
        created_at as sale_revision_created_at
    from {{ ref('stg_sale_revisions') }}
),

revision_items as (
    select
        item_id,
        sale_revision_id,
        product_id,
        coalesce(item_quantity, 0) as item_quantity,
        coalesce(item_unit_cost, 0) as item_unit_cost,
        coalesce(item_unit_price, 0) as item_unit_price,
        coalesce(item_line_total, 0) as item_line_total,
        cast(
            coalesce(item_unit_cost, 0) * coalesce(item_quantity, 0)
            as decimal(18, 3)
        ) as item_cost_total,
        coalesce(item_line_total, 0)
            - cast(
                coalesce(item_unit_cost, 0) * coalesce(item_quantity, 0)
                as decimal(18, 3)
            ) as item_gross_profit
    from {{ ref('stg_sale_revision_items') }}
),

final as (
    select
        s.sale_id,
        r.sale_revision_id as current_sale_revision_id,
        r.sale_revision_number as current_revision_number,
        r.sale_revision_created_at,
        ri.item_id,
        ri.product_id,
        ri.item_quantity,
        ri.item_unit_cost,
        ri.item_unit_price,
        ri.item_line_total,
        ri.item_cost_total,
        ri.item_gross_profit
    from sales s
    inner join revisions r
        on s.current_revision_id = r.sale_revision_id
    inner join revision_items ri
        on s.current_revision_id = ri.sale_revision_id
    
    where ri.item_cost_total >= 0
)

select * from final
