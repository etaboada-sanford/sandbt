select
    [Id] as dim_d365_whsinventstatus_sk
    , recid as whsinventstatus_recid
    , inventstatusid
    , name
    , inventstatusblocking
    , partition
    , [IsDelete]
    , upper(dataareaid) as whsinventstatus_dataareaid
from {{ source('fno', 'whsinventstatus') }}
where [IsDelete] is null
