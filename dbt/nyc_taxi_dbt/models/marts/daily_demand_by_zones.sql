select
    date_trunc('day', pickup_datetime) as trip_date
    , z1.borough
    , z1.zone
    , count(*) as total_trips
    , sum(fare_amount) as total_fare_revenue
    , sum(total_amount) as total_revenue
    , avg(tip_amount) as avg_tip_amount
    , avg(passenger_count) as avg_passenger
    , avg(trip_distance) as avg_trip_distance
from {{ref('fct_trips')}} as f
left join {{ref('dim_zone')}} as z1
on f.pulocationid = z1.locationid
group by 1, 2, 3