with standardized as (
    select
    id as device_id,
    name as device_name,
    created_at,
    last_seen_at

    from {{source('ppa_dbt','devices')}}
)

select * from standardized