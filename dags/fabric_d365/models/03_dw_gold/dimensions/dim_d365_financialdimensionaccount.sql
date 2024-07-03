select 

*
,

concat(
    coalesce(mainaccountname,'') , 
    '-',coalesce(d1_businessunit_name,''),
    '-',coalesce(d2_costcenter_name,''), 
    '-',coalesce(d4_function_name,''),
    '-',coalesce(d3_department_name,''),
    '-',coalesce(d5_customer_name,''),
    '-',coalesce(d6_vendor_name,''),
    '-',coalesce(d7_projectvalue,''),
    '-',coalesce(d8_legalentityvalue,'')
) dimension_ledgeraccount_text
,
'gl_category_l1' gl_category_l1,
'gl_category_l2' gl_category_l2,
'gl_category_l3' gl_category_l3


from 

(
select
d.recid financialdimensionaccount_recid,
concat(
    coalesce(d.mainaccountvalue,'') , 
    '-',coalesce(d.d1_businessunitvalue,''),
    '-',coalesce(d.d2_costcentervalue,''), 
    '-',coalesce(d.d4_functionvalue,''),
    '-',coalesce(d.d3_departmentvalue,''),
    '-',coalesce(d.d5_customervalue,''),
    '-',coalesce(d.d6_vendorvalue,''),
    '-',coalesce(d.d7_projectvalue,''),
    '-',coalesce(d.d8_legalentityvalue,'')
) dimension_ledgeraccount,
d.mainaccount,
d.mainaccountvalue,
m.name mainaccountname,
d.d1_businessunit,
d.d1_businessunitvalue,
d1pt.name d1_businessunit_name,

d.d2_costcenter ,
d.d2_costcentervalue ,
d2pt.name  d2_costcenter_name,

d.d4_function ,
d.d4_functionvalue ,
d4.description d4_function_name,    
    
d.d3_department ,
d.d3_departmentvalue ,
d3.description d3_department_name,



d.d5_customer ,
d.d5_customervalue ,
d5pt.name d5_customer_name,

d.d6_vendor,
d.d6_vendorvalue ,
d6pt.name d6_vendor_name,

d.d7_project,
d.d7_projectvalue,
--d7.name d7_project_name,
d.d8_legalentity,
d.d8_legalentityvalue,d.partition,
d.IsDelete


--select *
from {{ source('fno', 'dimensionattributevaluecombination') }}  d

left join {{ source('fno', 'mainaccount') }}  m on d.mainaccount = m.recid 

left join {{ source('fno', 'omoperatingunit') }}  d1 on d.d1_businessunit = d1.recid
left  join {{ source('fno', 'dirpartytable') }}  d1pt on (d1.recid) = (d1pt.recid)  
   

left join {{ source('fno', 'omoperatingunit') }}  d2 on d.d2_costcenter = d2.recid
left  join {{ source('fno', 'dirpartytable') }}  d2pt on (d2.recid) = (d2pt.recid)

left  join {{ source('fno', 'dimensionfinancialtag') }} d3 on d.d3_department = d3.recid
        --d.d3_departmentvalue   = d3._value

left  join {{ source('fno', 'dimensionfinancialtag') }} d4 on d.d4_function = d4.recid     
    --d.d4_functionvalue   = d4._value

left join {{ source('fno', 'custtable') }}  d5 on d.d5_customer = d5.recid 
   left  join {{ source('fno', 'dirpartytable') }}  d5pt on (d5.party) = (d5pt.recid)  

left join {{ source('fno', 'vendtable') }}  d6 on d.d6_vendor = d6.recid
   left  join {{ source('fno', 'dirpartytable') }}  d6pt on (d6.party) = (d6pt.recid)

where d.IsDelete is null

    )
    
    x



