select
    [Id] as dim_d365_company_sk
    , company_recid
    , party_recid
    , parentorganization
    , company_dataarea
    , parent_co_dataarea
    , company
    , company_name
    , parent_company_name
    , company_path
    , coregnum
    , bank
    , hierarchytypename
    , company_path_level
from {{ source('stage', 'stg_d365_company') }}
