with raw as (
    select *
    from {{ source('raw', 'raw_nyc_taxi_tripdata')}}
),
type_change as (
    select
        vendorid::int as vendorid
        , tpep_pickup_datetime::timestamp as tpep_pickup_datetime
        , tpep_dropoff_datetime::timestamp as tpep_dropoff_datetime
        , passenger_count::int as passenger_count
        , trip_distance::float as trip_distance
        , ratecodeid::int as ratecodeid
        , CASE WHEN store_and_fwd_flag = 'Y' THEN true ELSE false END as store_and_fwd_flag
        , pulocationid::int as pulocationid
        , dolocationid::int as dolocationid
        , payment_type::int as payment_type
        , fare_amount::number as fare_amount
        , extra::number as extra
        , mta_tax::number as mta_tax
        , tip_amount::number as tip_amount
        , tolls_amount::number as tolls_amount
        , improvement_surcharge::number as improvement_surcharge
        , total_amount::number as total_amount
        , congestion_surcharge::number as congestion_surcharge
        , airport_fee::number as airport_fee
        , cbd_congestion_fee::number as cbd_congestion_fee
        , year_month
        , loaded_at::datetime as loaded_at
    from raw
),
cleaned as (
    select *
    from type_change
    where 
        vendorid is not null
        and tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and trip_distance > 0
        and ratecodeid is not null
        and pulocationid is not null
        and dolocationid is not null 
        and passenger_count >= 0
        and fare_amount >= 0
        and extra >= 0
        and mta_tax >= 0
        and tip_amount >= 0
        and tolls_amount >= 0
        and total_amount >= 0
        and (airport_fee >= 0 or airport_fee is null)
        and (congestion_surcharge >= 0 or congestion_surcharge is null)
        and (cbd_congestion_fee >= 0 or cbd_congestion_fee is null)
),
duplicate as (
    select *
        , row_number() over(partition by vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid, fare_amount 
        order by loaded_at desc) as dup
    from cleaned
),
clean_duplicate as (
    select * exclude dup
    from duplicate
    where dup = 1
)
select * from clean_duplicate