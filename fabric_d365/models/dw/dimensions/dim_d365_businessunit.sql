    select distinct
        d.d1_businessunit as businessunitid,
        d.d1_businessunitvalue as businessunitvalue,
        d1pt.name as businessunit_name,
        d1.omoperatingunittype,
        d1.IsDelete,
        concat(upper(d.d1_businessunitvalue), ' - ', coalesce(d1pt.name, ''))
            as businessunit
    from {{ source('dbo', 'dimensionattributevaluecombination') }} as d
    inner join {{ source('dbo', 'omoperatingunit') }} as d1
            on d.d1_businessunit = d1.recid
    inner join {{ source('dbo', 'dirpartytable') }}  as d1pt
            on d1.recid = d1pt.recid
    where
        d.IsDelete is null
