{{ config(
    tags=["stg_d365_dimensionattributevalueset"]
) }}

select
    d.[Id] as dim_d365_financialdimensionvalueset_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d2_costcenter
    , d.d2_costcentervalue
    , d2pt.name as d2_costcenter_name
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'omoperatingunit') }} as d2 on d.d2_costcenter = d2.recid
left join {{ source('fno', 'dirpartytable') }} as d2pt on d2.recid = d2pt.recid
where
    d.[IsDelete] is null
