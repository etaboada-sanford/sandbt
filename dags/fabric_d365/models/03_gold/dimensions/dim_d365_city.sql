select
    [Id] as dim_d365_city_sk
    , recid as city_recid
    , citykey
    , description as city_desc
    , name as city_name
    , stateid
    , partition
    , [IsDelete]
    , upper(countryregionid) as countryregionid
from
    {{ source('fno', 'logisticsaddresscity') }}
where
    [IsDelete] is null
