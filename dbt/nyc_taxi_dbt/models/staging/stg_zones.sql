with raw as (
    select *
    from {{ref('taxi_zone_lookup')}}
),
renamed as (
    select
        locationid::int as locationid
        , borough::varchar as borough
        , zone::varchar as zone
        , service_zone::varchar as service_zone
    from raw
)
select * from renamed