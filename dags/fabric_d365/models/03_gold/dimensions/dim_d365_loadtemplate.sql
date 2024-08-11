/* LoadTemplate for TEU's */
select
    [Id] as dim_d365_loadtemplate_sk
    , recid as loadtemplate_recid
    , loadtemplateid
    , equipmentcode
    , loadheight
    , loaddepth
    , loadwidth
    , loadmaxvolume
    , loadmaxweight
    /* ISO Equipmentcode determines length
    -- 1st digit 2 = 20ft, 4 = 40ft
    -- 2nd digit is height
    -- chars 3 & 4 are type G general purpose, R refrigerated, T Tank
    */
    , case
        when left(equipmentcode, 1) = '2' then 1
        when left(equipmentcode, 1) = '4' then 2
    end as teu
    , upper(dataareaid) as loadtemplate_dataareaid
from {{ source('fno', 'whsloadtemplate') }}
