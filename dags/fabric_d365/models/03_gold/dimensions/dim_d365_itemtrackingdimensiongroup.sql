select
    tdg.[Id] as dim_d365_itemtrackingdimensiongroup_sk
    , tdg.recid as itemtrackingdimensiongroup_recid
    , tdg.name as itemtrackingdimensiongroupid
    , tdg.description as itemtrackingdimensiongroup_name
    , tdg.captureserial
    , tdg.isserialatconsumptionenabled
    , tdg.isserialnumbercontrolenabled
    , tdg.[IsDelete]

from {{ source('fno', 'ecorestrackingdimensiongroup') }} as tdg
where tdg.[IsDelete] is null
