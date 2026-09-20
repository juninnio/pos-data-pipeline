with devices as (
    select
    device_id,
    device_name,
    created_at,
    last_seen_at

    from {{ref('stg_devices')}}
)

select * from devices
