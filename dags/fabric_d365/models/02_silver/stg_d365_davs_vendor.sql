{{ config(
    tags=["stg_d365_dimensionattributevalueset"]
) }}

select
    d.[Id] as dim_d365_financialdimensionvalueset_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d6_vendor
    , d.d6_vendorvalue
    , d6pt.name as d6_vendor_name
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'vendtable') }} as d6 on d.d6_vendor = d6.recid
left join {{ source('fno', 'dirpartytable') }} as d6pt on d6.party = d6pt.recid
where
    d.[IsDelete] is null
