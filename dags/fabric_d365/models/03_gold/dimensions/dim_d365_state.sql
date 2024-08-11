select
    [Id] as dim_d365_state_sk
    , recid as state_recid
    , name as state_name
    , stateid
    , partition
    , [IsDelete]
    , upper(countryregionid) as countryregionid
from {{ source('fno', 'logisticsaddressstate') }}
where [IsDelete] is null
