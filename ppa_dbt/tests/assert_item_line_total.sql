select
    item_id,
    sale_revision_id,
    item_quantity,
    item_unit_price,
    item_line_total
from {{ ref('fct_sale_revision_items') }}
where
    item_quantity is null
    or item_unit_price is null
    or item_line_total is null
    or item_line_total <> item_quantity * item_unit_price