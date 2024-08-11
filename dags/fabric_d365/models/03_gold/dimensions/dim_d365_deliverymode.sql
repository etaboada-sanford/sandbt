select
    [Id] as dim_d365_deliverymode_sk
    , recid as deliverymode_recid
    , code as deliverymode
    , txt as deliverymode_desc
    , partition

    , [IsDelete]
    , upper(dataareaid) as deliverymode_dataareaid

from {{ source('fno', 'dlvmode') }}
where [IsDelete] is null
