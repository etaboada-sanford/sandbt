
select 

company_origin.dim_d365_company_sk dim_d365_company_origin_sk,
company_parent.dim_d365_company_sk dim_d365_company_parent_sk,
company_origin.company_dataarea,
company_origin.company_name,
company_parent.company_dataarea company_dataarea_parent,
company_parent.company_name company_name_parent


from {{source('mserp', 'ledgerconsolidatehist')}} h  

 left join {{ref('dim_d365_company')}} company_origin on company_origin.company_dataarea = upper(h.mserp_companyidorigin) 
 left join {{ref('dim_d365_company')}} company_parent on company_parent.company_dataarea = upper(h.mserp_dataareaid) 

where h.IsDelete is null