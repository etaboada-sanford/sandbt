select
    [Id] as dim_d365_currency_sk
    , recid as currency_recid
    , currencycode
    , symbol
    , roundingprecision
    , txt as currency_name
    , partition
    , [IsDelete]
from {{ source('fno', 'currency') }}
where [IsDelete] is null
