{{ config(
    tags=["stg_d365_dimensionattributevalueset"]
) }}

select
    d.[Id] as dim_d365_financialdimensionvalueset_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d5_customer
    , d.d5_customervalue
    , d5pt.name as d5_customer_name
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'custtable') }} as d5 on d.d5_customer = d5.recid
left join {{ source('fno', 'dirpartytable') }} as d5pt on d5.party = d5pt.recid
where
    d.[IsDelete] is null
