with ctw as (
    select
        [Id] as dim_d365_warehouselocation_sk
        , recid as warehouselocation_recid
        , wmslocationid as warehouselocationid
        , absoluteheight
        , aisleid
        , depth
        , height
        , volume
        , width
        , inputlocation
        , inventlocationid as warehouseid
        , locprofileid
        , locationtype
        , zoneid
        , level
        , partition
        , [IsDelete]
        , upper(dataareaid) as warehouselocation_dataareaid
    from {{ source('fno', 'wmslocation') }}
    where [IsDelete] is null
)

select
    *
    , case
        when zoneid like 'CHILLER%' then 1
        when warehouseid = 'HAV01' and warehouselocationid = 'RECV' then 1
        when warehouseid = 'NIML' and warehouselocationid = 'PRDDEF' then 1
        when warehouseid = 'BNE02' and warehouselocationid = 'INWCHI' then 1
        when warehouseid = 'BNE02' and warehouselocationid = 'CHIWIP' then 1
        when warehouseid = 'M04' and warehouselocationid = 'DEF' then 1
        when warehouseid = 'M12' and warehouselocationid = 'DEF' then 1
        when warehouseid = 'TIM01' and warehouselocationid = 'INWCHI' then 1
        else 0
    end as ischiller
from ctw
