with dimattrib as (
    select
        d.[Id] as dim_d365_financialdimensionvalueset_sk
        , d.recid as financialdimensionvalueset_recid
        , d.mainaccount
        , d.mainaccountvalue
        , m.name as mainaccountname
        , d.d1_businessunit
        , d.d1_businessunitvalue
        , d1pt.d1_businessunit_name
        , d.d2_costcenter

        , d.d2_costcentervalue
        , d2pt.d2_costcenter_name
        , d.d4_function

        , d.d4_functionvalue
        , d4.d4_function_name
        , d.d3_department

        , d.d3_departmentvalue
        , d3.d3_department_name
        , d.d5_customer

        , d.d5_customervalue
        , d5pt.d5_customer_name
        , d.d6_vendor

        , d.d6_vendorvalue
        , d6pt.d6_vendor_name
        , d.d7_project

        , d.d7_projectvalue
        , d.d8_legalentity

        , d.d8_legalentityvalue
        , d.partition

        , d.[IsDelete]
        , case
            when
                len(coalesce(d.mainaccountvalue, '')) > 0
                then concat(
                        coalesce(d.mainaccountvalue, '')
                        , '-'
                        , coalesce(d.d1_businessunitvalue, '')
                        , '-'
                        , coalesce(d.d2_costcentervalue, '')
                        , '-'
                        , coalesce(d.d4_functionvalue, '')
                        , '-'
                        , coalesce(d.d3_departmentvalue, '')
                        , '-'
                        , coalesce(d.d5_customervalue, '')
                        , '-'
                        , coalesce(d.d6_vendorvalue, '')
                        , '-'
                        , coalesce(d.d7_projectvalue, '')
                        , '-'
                        , coalesce(d.d8_legalentityvalue, '')
                    )
            when
                len(coalesce(d.mainaccountvalue, '')) = 0
                then concat(
                        coalesce(d.d1_businessunitvalue, '')
                        , '-'
                        , coalesce(d.d2_costcentervalue, '')
                        , '-'
                        , coalesce(d.d4_functionvalue, '')
                        , '-'
                        , coalesce(d.d3_departmentvalue, '')
                        , '-'
                        , coalesce(d.d5_customervalue, '')
                        , '-'
                        , coalesce(d.d6_vendorvalue, '')
                        , '-'
                        , coalesce(d.d7_projectvalue, '')
                        , '-'
                        , coalesce(d.d8_legalentityvalue, '')
                    )
        end as dimension_ledgeraccount
    from {{ source('fno', 'dimensionattributevalueset') }} as d
    left join {{ source('fno', 'mainaccount') }} as m on d.mainaccount = m.recid
    left join {{ ref('stg_d365_davs_businessunit') }} as d1pt on d.recid = d1pt.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_costcenter') }} as d2pt on d.recid = d2pt.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_function') }} as d4 on d.recid = d4.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_department') }} as d3 on d.recid = d3.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_customer') }} as d5pt on d.recid = d5pt.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_vendor') }} as d6pt on d.recid = d6pt.financialdimensionvalueset_recid
    where
        d.[IsDelete] is null
)

, dal as (
    select
        *
        , concat(
            coalesce(mainaccountname, '')
            , '-'
            , coalesce(d1_businessunit_name, '')
            , '-'
            , coalesce(d2_costcenter_name, '')
            , '-'
            , coalesce(d4_function_name, '')
            , '-'
            , coalesce(d3_department_name, '')
            , '-'
            , coalesce(d5_customer_name, '')
            , '-'
            , coalesce(d6_vendor_name, '')
            , '-'
            , coalesce(d7_projectvalue, '')
            , '-'
            , coalesce(d8_legalentityvalue, '')
        ) as dimension_ledgeraccount_text
    from dimattrib
)

select
    dim_d365_financialdimensionvalueset_sk
    , financialdimensionvalueset_recid
    , dimension_ledgeraccount
    , mainaccount
    , mainaccountvalue
    , mainaccountname
    , d1_businessunit
    , d1_businessunitvalue
    , d1_businessunit_name
    , d2_costcenter
    , d2_costcentervalue
    , d2_costcenter_name
    , d4_function
    , d4_functionvalue
    , d4_function_name
    , d3_department
    , d3_departmentvalue
    , d3_department_name
    , d5_customer
    , d5_customervalue
    , d5_customer_name
    , d6_vendor
    , d6_vendorvalue
    , d6_vendor_name
    , d7_project
    , d7_projectvalue
    , d8_legalentity
    , d8_legalentityvalue
    , partition
    , null as [IsDelete]
    , dimension_ledgeraccount_text
from dal
union all
select
    dim_d365_financialdimensionvalueset_sk
    , financialdimensionvalueset_recid
    , dimension_ledgeraccount
    , mainaccount
    , mainaccountvalue
    , mainaccountname
    , d1_businessunit
    , d1_businessunitvalue
    , d1_businessunit_name
    , d2_costcenter
    , d2_costcentervalue
    , d2_costcenter_name
    , d4_function
    , d4_functionvalue
    , d4_function_name
    , d3_department
    , d3_departmentvalue
    , d3_department_name
    , d5_customer
    , d5_customervalue
    , d5_customer_name
    , d6_vendor
    , d6_vendorvalue
    , d6_vendor_name
    , d7_project
    , d7_projectvalue
    , d8_legalentity
    , d8_legalentityvalue
    , partition
    , null as [IsDelete]
    , dimension_ledgeraccount_text
from
    {{ ref('int_nav_financialdimensionvalueset') }}
