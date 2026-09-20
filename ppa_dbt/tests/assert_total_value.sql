select
    sale_revision_id,
    sale_revision_subtotal,
    sale_revision_discount,
    sale_revision_total
from {{ ref('stg_sale_revisions') }}
where
    sale_revision_total != (sale_revision_subtotal - sale_revision_discount)