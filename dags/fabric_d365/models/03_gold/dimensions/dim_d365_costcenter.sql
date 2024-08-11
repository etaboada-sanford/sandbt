select distinct
    d.[Id] as dim_d365_costcenter_sk
    , d.d2_costcenter as costcenterid
    , d.d2_costcentervalue as costcentervalue
    , d2pt.name as costcenter_name
    , d2.omoperatingunittype
    , d2.[IsDelete]
    , concat(upper(d.d2_costcentervalue), ' - ', coalesce(d2pt.name, '')) as costcenter
from {{ source('fno', 'dimensionattributevaluecombination') }} as d
inner join
    {{ source('fno', 'omoperatingunit') }} as d2
    on d.d2_costcenter = d2.recid
inner join
    {{ source('fno', 'dirpartytable') }} as d2pt
    on cast(d2.recid as varchar) = cast(d2pt.recid as varchar)
where d.[IsDelete] is null and d2.[IsDelete] is null
