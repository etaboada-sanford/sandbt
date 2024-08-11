select
    d.[Id] as dim_d365_financialdimensionvalueset_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d1_businessunit
    , d.d1_businessunitvalue
    , d1pt.name as d1_businessunit_name
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join
    {{ source('fno', 'omoperatingunit') }} as d1
    on d.d1_businessunit = d1.recid
left join
    {{ source('fno', 'dirpartytable') }} as d1pt
    on d1.recid = d1pt.recid
where
    d.[IsDelete] is null
