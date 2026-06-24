select
    date_trunc('day', pickup_datetime) as trip_date
    , v.vendor_name
    , count(*) as total_trips
    , sum(total_amount) as total_revenue
    , avg(trip_distance) as avg_trip_distance
from {{ref('fct_trips')}} as f
left join {{ref('dim_vendor')}} as v 
on f.vendorid = v.vendorid
group by 1, 2