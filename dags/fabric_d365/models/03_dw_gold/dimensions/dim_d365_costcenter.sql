select distinct
       d.d2_costcenter costcenterid,
       d.d2_costcentervalue costcentervalue,
       d2pt.name costcenter_name,
       concat(upper(d.d2_costcentervalue),' - ',coalesce(d2pt.name,'')) costcenter,
       d2.omoperatingunittype,
       d2.IsDelete
    from {{ source('fno', 'dimensionattributevaluecombination') }} d
    join {{ source('fno', 'omoperatingunit') }} d2 on d.d2_costcenter = d2.recid
    join {{ source('fno', 'dirpartytable') }} d2pt on cast(d2.recid as varchar) = cast(d2pt.recid as varchar)
    where d.IsDelete is null and d2.IsDelete is null