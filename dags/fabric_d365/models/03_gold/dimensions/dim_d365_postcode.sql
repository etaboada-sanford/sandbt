select
    [Id] as dim_d365_postcode_sk
    , recid as postcode_recid
    , city
    , cityalias
    , cityrecid
    , district
    --county,
    , districtname
    , streetname
    , state
    , zipcode as postcode
    , partition
    , [IsDelete]
    , upper(countryregionid) as countryregionid

from {{ source('fno', 'logisticsaddresszipcode') }}
where [IsDelete] is null
