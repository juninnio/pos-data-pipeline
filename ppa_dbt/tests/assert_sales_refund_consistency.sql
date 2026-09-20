select
    sale_id,
    sale_status,
    refunded_at,
    is_fully_refunded
from {{ ref('fct_sales') }}
where
    (sale_status = 'refunded'
        and (refunded_at is null or is_fully_refunded = false))
    or
    (sale_status != 'refunded'
        and (refunded_at is not null or is_fully_refunded = true))