select
    sale_id,
    current_sale_revision_id,
    item_line_total,
    item_cost_total,
    item_gross_profit
from {{ ref('fct_final_sale_revisions') }}
where
    item_gross_profit is null
    or
    item_gross_profit <> item_line_total - item_cost_total