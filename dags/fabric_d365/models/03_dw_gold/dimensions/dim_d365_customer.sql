with ctcust as (
    select
        c.recid customer_recid,
        c.salesdistrictid ,

        sdg.description salesdistrictgroupdesc,
        sdg.dataareaid sdgdataareaid,
   
    
        c.party,
        c.accountnum,
        coalesce(c.invoiceaccount, c.accountnum) invoiceaccount,
        c.ouraccountnum,
        c.creditmax,
        --c.creditrating,
        --c.custclassificationid,
        c.custgroup,
        upper(c.dataareaid) dataareaid,
        c.dlvmode,
        c.dlvterm,
        c.lineofbusinessid,
        c.paymmode,
        c.pricegroup,



        --c.salesgroup,
        c.salespoolid,
        c.segmentid,
        sgm.description segment_description,
        c.statisticsgroup,
        c.subsegmentid,
        c.currency customer_currency,
        c.partycountry,
        c.partystate,
        c.orgid,

        pt.recid party_recid,
        --pt.isactive,
        'missing' as isactive,

        coy.coregnum,
        pt.partynumber,
        --pt.orgnumber,
        'missing' as orgnumber,
        pt.name,
        pt.namealias,
        -- pt.namesequence,
        'missing' as namesequence,
        -- pt.namecontrol,
        'missing' as namecontrol,
        pt.primarycontactphone,
        pt.primaryaddresslocation,
        -- pt.numberofemployees,
        'missing' as numberofemployees,
        -- pt.dataarea,
        'missing' as dataarea,
        pt.createddatetime,
        pt.createdby,
        pt.modifieddatetime,
        pt.modifiedby,
        c.partition,
        c.blocked blockedid,
        --eb.enumitemlabel blocked,
        'missing' blocked,
        c.defaultdimension defaultdimension_recid,
        c.IsDelete
    from {{ source('fno', 'custtable') }} as c
    left join {{ source('fno', 'dirpartytable') }} as pt on c.party = pt.recid
    left join {{ source('fno', 'companyinfo') }} as coy on c.dataareaid = coy.dataareaid
    left join {{ source('fno', 'smmbusrelsalesdistrictgroup') }} as sdg on c.salesdistrictid = sdg.salesdistrictid and upper(c.dataareaid) = upper(sdg.dataareaid) 
    left join {{ source('fno', 'smmbusrelsegmentgroup') }} as sgm on c.segmentid = sgm.segmentid and upper(c.dataareaid) = upper(sgm.dataareaid) 
    
    /*
    left join
        'dxc_enumstable' as eb
        on
            eb.enumname = 'CustVendorBlocked'
            and c.blocked = eb.enumitemvalue
            and eb.IsDelete = false */
    where
        c.IsDelete is null
),

custfinal as (
    select
        customer_recid as customer_recid,
        accountnum as accountnum,
        invoiceaccount as invoiceaccount,
        ouraccountnum as ouraccountnum,
        orgnumber as orgnumber,
        name,
        party as party_recid,
        custgroup as custgroup,
        salesdistrictid as salesdistrictid,
        salesdistrictgroupdesc as salesdistrictgroupdesc,
        blocked as blocked,
        pricegroup as pricegroup,
        defaultdimension_recid as defaultdimension_recid,
        dataareaid as customer_dataareaid,
        is_deleted as is_deleted
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
        is_deleted as is_deleted
    from
         'int_nav_dummy_custgroup'  */
) 
select
    cu.*,
    pcu.name as parent_customer_name,
    cu.invoiceaccount as parent_customer_account
from custfinal as cu
left join custfinal as pcu on cu.invoiceaccount = pcu.accountnum and cu.customer_dataareaid = pcu.customer_dataareaid
