select
    recid as city_recid,
    citykey,
    upper(countryregionid) as countryregionid,
    description as city_desc,
    name as city_name,
    stateid,
    partition,
    IsDelete
from
    {{ source('fno', 'logisticsaddresscity') }}
where
    IsDelete is null
    