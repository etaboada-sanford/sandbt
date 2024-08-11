select
    cr.[Id] as dim_d365_country_sk
    , cr.recid as country_recid
    , cr.isocode as country_isocode
    , crt.shortname as country_name
    , crt.longname as country_lname
    , cr.partition
    , cr.[IsDelete]
    , upper(cr.countryregionid) as countryregionid
    , replace(mc.tm1_continent, ' Sales Destination', '') as continent
from {{ source('fno', 'logisticsaddresscountryregion') }} as cr
inner join {{ source('fno', 'logisticsaddresscountryregiontranslation') }} as crt
    on upper(cr.countryregionid) = upper(crt.countryregionid)
        and crt.languageid = 'en-NZ'
        and crt.[IsDelete] is null
left join {{ ref('stg_map_d365_nav_country') }} as mc on upper(crt.countryregionid) = upper(mc.d365_country_code)
where cr.[IsDelete] is null
