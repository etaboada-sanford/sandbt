select
    [Id] as dim_d365_inventitemprice_sk
    , recid
    , costingtype
    , itemid
    , markup
    , priceallocatemarkup
    , pricecalcid
    , priceqty
    , pricetype
    , priceunit
    , activationdate
    , createddatetime
    , unitid
    , versionid
    , dataareaid
    , dxc_inventitempricesim
    , [IsDelete]
    , round(price, 2) as price

from {{ source('fno', 'inventitemprice') }}

where [IsDelete] is null
