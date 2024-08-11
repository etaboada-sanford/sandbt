select
    [Id] as dim_d365_itemcoveragegroup_sk
    , recid as itemcoveragegroup_recid
    , reqgroupid as itemcoveragegroupid
    , name as itemcoveragegroup_name
    , actioncalc
    , actiontimefence
    , capacitytimefence
    , covperiod
    , covrule
    , covtimefence
    , explosiontimefence
    , futurescalc
    , masterplantimefence
    , maxnegativedays
    , maxpositivedays
    , partition
    , [IsDelete]
    , upper(dataareaid) as itemcoveragegroup_dataareaid
from {{ source('fno', 'reqgroup') }}
where [IsDelete] is null
