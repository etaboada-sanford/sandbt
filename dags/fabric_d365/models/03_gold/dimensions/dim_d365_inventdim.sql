select
    [Id] as dim_d365_inventdim_sk
    , recid as inventdim_recid
    , inventdimid
    , inventbatchid
    , inventlocationid
    , wmslocationid
    , inventserialid
    , inventsiteid
    , inventstatusid
    , inventversionid
    , inventstyleid
    , licenseplateid
    , wmspalletid
    , '' as partitionid
    , createddatetime
    , [IsDelete]
    , upper(dataareaid) as inventdim_dataareaid
from
    {{ source('fno', 'inventdim') }}
where
    [IsDelete] is null
