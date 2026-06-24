select
    -- keys
    vendorid
    , ratecodeid
    , payment_type as paymentid
    , pulocationid
    , dolocationid
    , tpep_pickup_datetime::date as pickup_date
    , tpep_pickup_datetime as pickup_datetime
    , tpep_dropoff_datetime as dropoff_datetime

    -- measures
    , trip_distance
    , passenger_count
    , fare_amount
    , extra
    , mta_tax
    , tip_amount
    , tolls_amount
    , airport_fee
    , total_amount
    , improvement_surcharge
    , congestion_surcharge
    , cbd_congestion_fee
from {{ref('stg_trips')}}
where year(tpep_pickup_datetime) >= 2009