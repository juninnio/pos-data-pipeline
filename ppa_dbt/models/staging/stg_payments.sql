with standardized as (
    select
        id as payment_id,
        sale_id,
        revision_id as sale_revision_id,
        payment_method_id,
        payment_method_name_snapshot,
        cast(amount_tendered_minor as int) as payment_amount_tendered,
        cast(change_minor as int) as payment_change,
        created_at
    from {{source('ppa_dbt','payments')}}
)

select * from standardized