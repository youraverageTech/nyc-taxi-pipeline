select
    date_trunc('day', pickup_date) as trip_date
    , p.payment_name
    , count(*) as total_trips
    , sum(total_amount) as total_revenue
from {{ref('fct_trips')}} as f
left join {{ref('dim_payment')}} as p 
on f.paymentid = p.paymentid
group by 1, 2