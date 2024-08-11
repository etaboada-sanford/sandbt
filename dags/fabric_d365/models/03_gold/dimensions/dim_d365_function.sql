with ct as (
    select distinct
        d4.recid as functionid
        , d4.value as functionvalue
        , d4.description as function_name
        , d4.partition
        , d4.[IsDelete]
    from {{ source('fno', 'dimensionattributevaluecombination') }} as d
    inner join {{ source('fno', 'dimensionfinancialtag') }} as d4 on d.d4_function = d4.recid
    where d4.[IsDelete] is null
        and d.[IsDelete] is null
)

select
    {{ dbt_utils.generate_surrogate_key(['ct.functionid']) }} as dim_d365_function_sk
    , ct.*
from ct
