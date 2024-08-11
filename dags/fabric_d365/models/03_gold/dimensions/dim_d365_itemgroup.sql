select
    [Id] as dim_d365_itemgroup_sk
    , recid as itemgroup_recid
    , itemgroupid
    , name as itemgroup_name
    , dxc_israwmaterial
    , partition
    , [IsDelete]
    , upper(dataareaid) as itemgroup_dataareaid
from {{ source('fno', 'inventitemgroup') }}
where [IsDelete] is null
