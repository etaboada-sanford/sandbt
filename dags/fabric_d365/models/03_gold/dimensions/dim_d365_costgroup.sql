select
    cg.[Id] as dim_d365_costgroup_sk
    , cg.recid as costgroup_recid
    , cg.costgroupid
    , cg.name as costgroup_name
    , cg.dxc_bomfinishedgooditem
    , cg.dxc_bominputitem
    , e.[LocalizedLabel] as costgrouptype
    , cg.[IsDelete]
    , upper(cg.dataareaid) as costgroup_dataareaid
from {{ source('fno', 'bomcostgroup') }} as cg
left join {{ source('fno', 'GlobalOptionsetMetadata') }} as e
    on
        cg.costgrouptype = e.[Option]
        and e.[OptionSetName] = 'costgrouptype'
        and e.[EntityName] = 'bomcostgroup'
where cg.[IsDelete] is null
