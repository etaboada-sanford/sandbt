{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_productionorder_sk']
) }}

select
    pt.[Id] as dim_d365_productionorder_sk
    , pt.recid as productionorder_recid
    , pt.prodid
    , pt.dxc_parentprodid as parentprodid
    , pt.prodpoolid
    , eppt.[LocalizedLabel] as prodpostingtype
    , eps.[LocalizedLabel] as prodstatus
    , epbs.[LocalizedLabel] as backorderstatus
    , pt.dxc_salesid as salesid
    , pt.dxc_salesinventtransid as salesinventtransid
    , sl.qtyordered as salesline_qtyordered
    , pt.bomid
    , pt.collectreflevel
    , pt.collectrefprodid
    , pt.latestscheddirection
    , pt.schedstatus
    , pt.inventdimid
    , pt.inventrefid
    , pt.inventreftransid
    , pt.inventtransid
    , pt.itemid
    , pt.qtycalc
    , pt.qtysched
    , pt.qtystup
    , pt.remaininventphysical
    , pt.dxc_salescarriercode as salescarriercode
    , pt.dxc_salespoolid as salespoolid
    , pt.dxc_packagingunit as packagingunit
    , pt.dxc_packagingqty as packagingqty
    , case when pt.dxc_saleslinenumber = 0 then null else pt.dxc_saleslinenumber end as saleslinenumber
    , upper(sl.salesunit) as salesline_unit
    , round(case when upper(sl.salesunit) = 'LB' then sl.qtyordered * 0.45359237 else sl.qtyordered end, 3) as salesline_qtyordered_kg
    , convert(date, pt.bomdate) as bomdate
    , convert(date, pt.calcdate) as calcdate
    , case when convert(date, pt.dlvdate) = '1900-01-01' then null
        else convert(datetime2(0), concat(convert(varchar(10), convert(date, pt.dlvdate), 120), ' ', convert(varchar(8), dateadd(second, pt.dlvtime, 0), 108)))
    end as deliverydatetime
    , case when convert(date, pt.finisheddate) = '1900-01-01' then null else convert(date, pt.finisheddate) end as finisheddate
    , convert(datetime2(0), concat(convert(varchar(10), convert(date, pt.latestscheddate), 120), ' ', convert(varchar(8), dateadd(second, pt.latestschedtime, 0), 108))) as latestscheduled_datetime
    , case when convert(date, pt.scheddate) = '1900-01-01' then null else convert(date, pt.scheddate) end as scheddate
    , case when convert(date, pt.schedend) = '1900-01-01' then null else convert(date, pt.schedend) end as schedenddate
    , case when convert(date, pt.schedstart) = '1900-01-01' then null else convert(date, pt.schedstart) end as schedstartdate
    , convert(time(0), dateadd(second, pt.schedfromtime, 0)) as schedfromtime
    , convert(time(0), dateadd(second, pt.schedtotime, 0)) as schedtotime
    , case when convert(date, pt.realdate) = '1900-01-01' then null else convert(date, pt.realdate) end as realdate
    , case when convert(date, pt.releaseddate) = '1900-01-01' then null else convert(date, pt.releaseddate) end as releaseddate
    , case when convert(date, pt.dlvdate) > '1900-01-01' then convert(date, pt.dlvdate)
        else convert(date, pt.releaseddate)
    end as reportdate
    , coalesce(eirt.[LocalizedLabel], 'None') as inventreftype
    , upper(pt.dataareaid) as prod_dataareaid
    , pt.versionnumber
    , pt.sysrowversion
from {{ source('fno', 'prodtable') }} as pt

left join
    {{ source('fno', 'salesline') }}
        as sl
    on pt.dxc_salesid = sl.salesid
        and pt.dxc_saleslinenumber = sl.linenum
        and pt.dataareaid = sl.dataareaid
        and sl.[IsDelete] is null

/* ProdStatus -> ProdStatus */
left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as eps
    on eps.[OptionSetName] = 'prodstatus'
        and pt.prodstatus = eps.[Option]
        and eps.[EntityName] = 'prodtable'
/* backorderstatus -> ProdBackStatus */
left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as epbs
    on epbs.[OptionSetName] = 'backorderstatus'
        and pt.backorderstatus = epbs.[Option]
        and epbs.[EntityName] = 'prodtable'
/* ProdPostingType -> ProdPostingType */
left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as eppt
    on eppt.[OptionSetName] = 'prodpostingtype'
        and pt.prodpostingtype = eppt.[Option]
        and eppt.[EntityName] = 'prodtable'
/* InventRefType -> InventRefType */
left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as eirt
    on eirt.[OptionSetName] = 'inventreftype'
        and pt.inventreftype = eirt.[Option]
        and eirt.[EntityName] = 'prodtable'
{%- if is_incremental() %}
    where pt.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pt.[IsDelete] is null
{% endif %}
