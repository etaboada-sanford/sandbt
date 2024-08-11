select
    [Id] as dim_d365_itembuyergroup_sk
    , recid as itembuyergroup_recid
    , [group] as itembuyergroupid
    , description as itembuyergroup_name
    , partition
    , [IsDelete]
    , upper(dataareaid) as itembuyergroup_dataareaid
from {{ source('fno', 'inventbuyergroup') }}
where [IsDelete] is null
