select
    [Id] as dim_d365_addresscontactrole_sk
    , recid as addresscontactrole_recid
    , iscontactinfo
    , ispostaladdress
    , name as addresscontactrole_name
    , type as addresscontactrole_type
    , partition
    , [IsDelete]
from {{ source('fno', 'logisticslocationrole') }}
where [IsDelete] is null
