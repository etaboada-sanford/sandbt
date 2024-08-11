{{ config(
    tags=["stg_d365_dimensionattributevalueset"]
) }}

select
    d.[Id] as dim_d365_financialdimensionvalueset_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d4_function
    , d.d4_functionvalue
    , d4.description as d4_function_name
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'dimensionfinancialtag') }} as d4 on d.d4_function = d4.recid
where
    d.[IsDelete] is null
