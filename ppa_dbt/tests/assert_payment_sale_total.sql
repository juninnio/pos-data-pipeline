select
    payment_id,
    sale_id,
    sale_revision_id,
    payment_amount_tendered,
    payment_change,
    payment_amount_net,
    sale_total
from {{ ref('fct_payments') }}
where
    payment_amount_net != sale_total
    or
    payment_amount_net IS NULL
    or
    sale_total IS NULL