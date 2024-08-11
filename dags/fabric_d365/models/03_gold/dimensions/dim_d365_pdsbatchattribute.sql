select
    attr.[Id] as dim_d365_pdsbatchattribute_sk
    , attr.recid as pdsbatchattribute_recid
    , attr.itemid
    , attr.pdsbatchattribid
    , attr.[IsDelete]
    , attr.partition
    , upper(attr.inventbatchid) as inventbatchid
    , case
        when b.pdsbatchattribtype != 5 then attr.pdsbatchattribvalue
        else convert(varchar, convert(date, dateadd(day, convert(int, attr.pdsbatchattribvalue), '1900-01-01')), 105)
    end as pdsbatchattribvalue
    , upper(attr.dataareaid) as pdsbatchattribute_dataareaid
from {{ source('fno', 'pdsbatchattributes') }} as attr
inner join {{ source('fno', 'pdsbatchattrib') }} as b on attr.pdsbatchattribid = b.pdsbatchattribid and b.[IsDelete] is null and upper(attr.dataareaid) = upper(b.dataareaid)
inner join {{ source('fno', 'GlobalOptionsetMetadata') }} as e on b.pdsbatchattribtype = e.[Option] and e.[OptionSetName] = 'pdsbatchattribtype' and e.[EntityName] = 'pdsbatchattrib'
