select
    sdg.[Id] as dim_d365_itemstoragedimensiongroup_sk
    , sdg.recid as itemstoragedimensiongroup_recid
    , sdg.name as itemstoragedimensiongroupid
    , sdg.description as itemstoragedimensiongroup_name
    , sdg.[IsDelete]
from {{ source('fno', 'ecoresstoragedimensiongroup') }} as sdg
where sdg.[IsDelete] is null
