with ctcust as (
    select
        c.[Id] as dim_d365_customer_sk
        , c.recid as customer_recid
        , c.salesdistrictid

        , sdg.description as salesdistrictgroupdesc
        , sdg.dataareaid
            as sdgdataareaid


        , c.party
        , c.accountnum
        , c.ouraccountnum
        , c.creditmax
        , c.custgroup
        --c.creditrating,
        --c.custclassificationid,
        , c.dlvmode
        , c.dlvterm
        , c.lineofbusinessid
        , c.paymmode
        , c.pricegroup
        , c.salespoolid
        , c.segmentid



        --c.salesgroup,
        , sgm.description as segment_description
        , c.statisticsgroup
        , c.subsegmentid
        , c.currency as customer_currency
        , c.partycountry
        , c.partystate
        , c.orgid
        , pt.recid as party_recid
        , 'missing'
            as isactive

        , coy.coregnum
        --pt.isactive,
        , pt.partynumber

        , 'missing' as orgnumber
        , pt.name
        --pt.orgnumber,
        , pt.namealias
        , 'missing' as namesequence
        , 'missing' as namecontrol
        -- pt.namesequence,
        , pt.primarycontactphone
        -- pt.namecontrol,
        , pt.primaryaddresslocation
        , 'missing' as numberofemployees
        , 'missing'
            as dataarea
        -- pt.numberofemployees,
        , pt.createddatetime
        -- pt.dataarea,
        , pt.createdby
        , pt.modifieddatetime
        , pt.modifiedby
        , c.partition
        , c.blocked as blockedid
        , 'missing' as blocked
        , c.defaultdimension
            as defaultdimension_recid
        --eb.enumitemlabel blocked,
        , c.[IsDelete]
        , coalesce(c.invoiceaccount, c.accountnum) as invoiceaccount
        , upper(c.dataareaid) as dataareaid
    from {{ source('fno', 'custtable') }} as c
    left join {{ source('fno', 'dirpartytable') }} as pt on c.party = pt.recid
    left join
        {{ source('fno', 'companyinfo') }} as coy
        on c.dataareaid = coy.dataareaid
    left join
        {{ source('fno', 'smmbusrelsalesdistrictgroup') }} as sdg
        on
            c.salesdistrictid = sdg.salesdistrictid
            and upper(c.dataareaid) = upper(sdg.dataareaid)
    left join
        {{ source('fno', 'smmbusrelsegmentgroup') }} as sgm
        on
            c.segmentid = sgm.segmentid
            and upper(c.dataareaid) = upper(sgm.dataareaid)

    /*
                        left join
                        'dxc_enumstable' as eb
                        on
                        eb.enumname = 'CustVendorBlocked'
                        and c.blocked = eb.enum_item_value
                        and eb.[IsDelete] = false */
    where
        c.[IsDelete] is null
)

, custfinal as (
    select
        customer_recid
        , accountnum
        , invoiceaccount
        , ouraccountnum
        , orgnumber
        , name
        , party as party_recid
        , custgroup
        , salesdistrictid
        , salesdistrictgroupdesc
        , blocked
        , pricegroup
        , defaultdimension_recid
        , dataareaid as customer_dataareaid
        , [IsDelete]
    from
        ctcust /*
                union all
                select
                dim_d365_customer_sk as dim_d365_customer_sk,
                customer_recid as customer_recid,
                accountnum as accountnum,
                invoiceaccount as invoiceaccount,
                ouraccountnum as ouraccountnum,
                orgnumber as orgnumber,
                name,
                party_recid as party_recid,
                custgroup as custgroup,
                salesdistrictid as salesdistrictid,
                salesdistrictgroupdesc as salesdistrictgroupdesc,
                blocked as blocked,
                pricegroup as pricegroup,
                defaultdimension_recid as defaultdimension_recid,
                customer_dataareaid as customer_dataareaid,
                [IsDelete] as [IsDelete]
                from
                'int_nav_dummy_custgroup'  */
)

select
    cu.*
    , pcu.name as parent_customer_name
    , cu.invoiceaccount as parent_customer_account
from custfinal as cu
left join
    custfinal as pcu
    on
        cu.invoiceaccount = pcu.accountnum
        and cu.customer_dataareaid = pcu.customer_dataareaid
