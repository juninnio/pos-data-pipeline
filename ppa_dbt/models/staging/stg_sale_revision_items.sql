with standardized as (
    select
        id as item_id,
        revision_id as sale_revision_id,
        product_id,
        product_name_snapshot,
        category_name_snapshot,
        cast(quantity as int) as item_quantity,
        cast(unit_cost_minor as int) as item_unit_cost,
        cast(unit_price_minor as int) as item_unit_price,
        cast(line_total_minor as int) as item_line_total
    from {{source('ppa_dbt', 'sale_revision_items')}}
)

select * from standardized