select
    date_trunc('day', pickup_datetime) as trip_date
    , hour(pickup_datetime) as pickup_hour
    , count(*) as total_trips
    , avg(passenger_count) as avg_passenger
from {{ref('fct_trips')}}
group by 1, 2