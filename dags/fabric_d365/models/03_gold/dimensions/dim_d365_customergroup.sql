select
    [Id] as dim_d365_customergroup_sk
    , recid as customergroup_recid
    , custgroup as customergroup
    , defaultdimension
    , name as customergroup_name
    , paymtermid
    , partition
    , [IsDelete]
    , upper(dataareaid) as customergroup_dataareaid
from {{ source('fno', 'custgroup') }}
where [IsDelete] is null
