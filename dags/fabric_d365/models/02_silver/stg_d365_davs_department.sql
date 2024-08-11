{{ config(
    tags=["stg_d365_dimensionattributevalueset"]
) }}

select
    d.[Id] as dim_d365_financialdimensionvalueset_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d3_department
    , d.d3_departmentvalue
    , d3.description as d3_department_name
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'dimensionfinancialtag') }} as d3 on d.d3_department = d3.recid
where
    d.[IsDelete] is null
