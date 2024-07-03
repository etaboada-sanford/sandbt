select 
distinct
d3.recid departmentid,
d3.value departmentvalue,
d3.description department_name,
concat(upper(d3.value),' - ',coalesce(d3.description,'')) department,
d3.partition,
d3.IsDelete
from {{ source('fno', 'dimensionattributevaluecombination') }} d
join {{ source('fno', 'dimensionfinancialtag') }} d3 on d.d3_department = d3.recid
where d.IsDelete is null and d3.IsDelete is null