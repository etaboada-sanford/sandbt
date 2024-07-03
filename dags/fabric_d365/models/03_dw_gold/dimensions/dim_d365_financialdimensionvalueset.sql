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

from 

(
select
d.recid financialdimensionvalueset_recid,

   case 
    when len(coalesce(d.mainaccountvalue,''))>0 then
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
        )
    when len(coalesce(d.mainaccountvalue,''))=0 then
    concat(
    coalesce(d.d1_businessunitvalue,''),
    '-',coalesce(d.d2_costcentervalue,''), 
    '-',coalesce(d.d4_functionvalue,''),
    '-',coalesce(d.d3_departmentvalue,''),
    '-',coalesce(d.d5_customervalue,''),
    '-',coalesce(d.d6_vendorvalue,''),
    '-',coalesce(d.d7_projectvalue,''),
    '-',coalesce(d.d8_legalentityvalue,'')
    )
    else
    null
    end
    
    
 dimension_ledgeraccount,
    
    
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
--d8.name d8_legalentity_name


--select  top 10  *
from {{ source('fno', 'dimensionattributevalueset') }} d 
--where mainaccount <>0

left join {{ source('fno', 'mainaccount') }}   m
 on d.mainaccount = m.recid 

left join {{ source('fno', 'omoperatingunit') }}  d1
 on d.d1_businessunit = d1.recid
left  join {{ source('fno', 'dirpartytable') }}   d1pt
 on (d1.recid) = (d1pt.recid)  
   

left join {{ source('fno', 'omoperatingunit') }}   d2 
on d.d2_costcenter = d2.recid
left  join {{ source('fno', 'dirpartytable') }}   d2pt 
on (d2.recid) = (d2pt.recid)

left  join {{ source('fno', 'dimensionfinancialtag') }} d3 
on d.d3_department = d3.recid
        --d.d3_departmentvalue   = d3._value

left  join {{ source('fno', 'dimensionfinancialtag') }}  d4 
on d.d4_function = d4.recid     
    --d.d4_functionvalue   = d4._value

left join {{ source('fno', 'custtable') }}   d5 
on d.d5_customer = d5.recid 
   left  join {{ source('fno', 'dirpartytable') }}   d5pt 
   on (d5.party) = (d5pt.recid)  

left join {{ source('fno', 'vendtable') }}   d6 
on d.d6_vendor = d6.recid
   left  join {{ source('fno', 'dirpartytable') }} d6pt 
   on (d6.party) = (d6pt.recid)

where d.IsDelete is null

    )
    
    x
--where x.financialdimensionaccount_recid in ( 5637202328,5637179483)

-- records for Customers not migrated from Navision but in DW Sales data

union all
select -999991 as financialdimensionvalueset_recid
    , 'A1----NAV00000---SANF' as dimension_ledgeraccount
    , 0 as mainaccount
    , null as mainaccountvalue
    , null as mainaccountname
    , 5637146830 as d1_businessunit
    , 'A1' as d1_businessunitvalue
    , 'Salmon' as d1_businessunit_name
    , 0 as d2_costcenter
    , null as d2_costcentervalue
    , null as d2_costcenter_name
    , 0 as d4_function
    , null as d4_functionvalue
    , null as d4_function_name
    , 0 as d3_department
    , null as d3_departmentvalue
    , null as d3_department_name
    , -999999 as d5_customer
    , 'NAV00000' as d5_customervalue
    , 'Navision not migrated' as d5_customer_name
    , 0 as d6_vendor
    , null as d6_vendorvalue
    , null as d6_vendor_name
    , 0 as d7_project
    , null as d7_projectvalue
    , 5637145326 as d8_legalentity
    , 'SANF' as d8_legalentityvalue
    , 5637144576 as partition
    , 0 as IsDelete
    , '-Salmon----Navision not migrated---SANF' as dimension_ledgeraccount_text
 union all
 select -999992 as financialdimensionvalueset_recid
    , 'A2----NAV00000---SANF' as dimension_ledgeraccount
    , 0 as mainaccount
    , null as mainaccountvalue
    , null as mainaccountname
    , 5637146831 as d1_businessunit
    , 'A2' as d1_businessunitvalue
    , 'Mussels' as d1_businessunit_name
    , 0 as d2_costcenter
    , null as d2_costcentervalue
    , null as d2_costcenter_name
    , 0 as d4_function
    , null as d4_functionvalue
    , null as d4_function_name
    , 0 as d3_department
    , null as d3_departmentvalue
    , null as d3_department_name
    , -999999 as d5_customer
    , 'NAV00000' as d5_customervalue
    , 'Navision not migrated' as d5_customer_name
    , 0 as d6_vendor
    , null as d6_vendorvalue
    , null as d6_vendor_name
    , 0 as d7_project
    , null as d7_projectvalue
    , 5637145326 as d8_legalentity
    , 'SANF' as d8_legalentityvalue
    , 5637144576 as partition
    , 0 as IsDelete
    , '-Mussels----Navision not migrated---SANF' as dimension_ledgeraccount_text
 union all
 select  -999993 as financialdimensionvalueset_recid
    , 'W1----NAV00000---SANF' as dimension_ledgeraccount
    , 0 as mainaccount
    , null as mainaccountvalue
    , null as mainaccountname
    , 5637146828 as d1_businessunit
    , 'W1' as d1_businessunitvalue
    , 'Wildcatch Deepwater' as d1_businessunit_name
    , 0 as d2_costcenter
    , null as d2_costcentervalue
    , null as d2_costcenter_name
    , 0 as d4_function
    , null as d4_functionvalue
    , null as d4_function_name
    , 0 as d3_department
    , null as d3_departmentvalue
    , null as d3_department_name
    , -999999 as d5_customer
    , 'NAV00000' as d5_customervalue
    , 'Navision not migrated' as d5_customer_name
    , 0 as d6_vendor
    , null as d6_vendorvalue
    , null as d6_vendor_name
    , 0 as d7_project
    , null as d7_projectvalue
    , 5637145326 as d8_legalentity
    , 'SANF' as d8_legalentityvalue
    , 5637144576 as partition
    , 0 as IsDelete
    , '-Wildcatch Deepwater----Navision not migrated---SANF' as dimension_ledgeraccount_text    
union all
 select -999994 as financialdimensionvalueset_recid
    , 'W2----NAV00000---SANF' as dimension_ledgeraccount
    , 0 as mainaccount
    , null as mainaccountvalue
    , null as mainaccountname
    , 5637164076 as d1_businessunit
    , 'W2' as d1_businessunitvalue
    , 'Wildcatch Inshore' as d1_businessunit_name
    , 0 as d2_costcenter
    , null as d2_costcentervalue
    , null as d2_costcenter_name
    , 0 as d4_function
    , null as d4_functionvalue
    , null as d4_function_name
    , 0 as d3_department
    , null as d3_departmentvalue
    , null as d3_department_name
    , -999999 as d5_customer
    , 'NAV00000' as d5_customervalue
    , 'Navision not migrated' as d5_customer_name
    , 0 as d6_vendor
    , null as d6_vendorvalue
    , null as d6_vendor_name
    , 0 as d7_project
    , null as d7_projectvalue
    , 5637145326 as d8_legalentity
    , 'SANF' as d8_legalentityvalue
    , 5637144576 as partition
    , 0 as IsDelete
    , '-Wildcatch Inshore----Navision not migrated---SANF' as dimension_ledgeraccount_text   
union all
 select -999995 as financialdimensionvalueset_recid
    , 'C1----NAV00000---SANF' as dimension_ledgeraccount
    , 0 as mainaccount
    , null as mainaccountvalue
    , null as mainaccountname
    , 5637146833 as d1_businessunit
    , 'C1' as d1_businessunitvalue
    , 'Corporate' as d1_businessunit_name
    , 0 as d2_costcenter
    , null as d2_costcentervalue
    , null as d2_costcenter_name
    , 0 as d4_function
    , null as d4_functionvalue
    , null as d4_function_name
    , 0 as d3_department
    , null as d3_departmentvalue
    , null as d3_department_name
    , -999999 as d5_customer
    , 'NAV00000' as d5_customervalue
    , 'Navision not migrated' as d5_customer_name
    , 0 as d6_vendor
    , null as d6_vendorvalue
    , null as d6_vendor_name
    , 0 as d7_project
    , null as d7_projectvalue
    , 5637145326 as d8_legalentity
    , 'SANF' as d8_legalentityvalue
    , 5637144576 as partition
    , 0 as IsDelete
    , '-Corporate----Navision not migrated---SANF' as dimension_ledgeraccount_text       

