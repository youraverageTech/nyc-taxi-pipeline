select
    locationid
    , borough
    , zone
    , service_zone
from {{ref('stg_zones')}}