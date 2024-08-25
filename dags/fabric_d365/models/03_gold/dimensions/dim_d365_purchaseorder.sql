{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_purchaseorder_sk']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['pt.recid']) }} as dim_d365_purchaseorder_sk
    , pt.recid as purchaseorder_recid
    , upper(pt.dataareaid) as purchaseorder_dataareaid
    , pt.purchid
    , pt.currencycode
    , pt.invoiceaccount
    , pt.orderaccount
    , pt.vendgroup
    , pt.vendorref
    , pt.deliveryname
    , pt.deliverydate
    , pt.deliverypostaladdress
    , pt.dlvmode
    , pt.dlvterm
    , pt.email
    , pt.defaultdimension
    , pt.purchname
    , pt.purchpoolid
    , pt.purchstatus as purchstatusid
    , eps.[LocalizedLabel] as purchstatus
    , pt.purchasetype as purchasetypeid
    , ept.[LocalizedLabel] as purchasetype
    , pt.inventsiteid as siteid
    , s.name as site_name
    , upper(s.dataareaid) as site_dataareaid
    , pt.inventlocationid as warehouseid
    , w.name as warehouse_name
    , upper(w.dataareaid) as warehouse_dataareaid
    , pt.intercompanyorder
    , upper(pt.intercompanycompanyid) as intercompanycompanyid
    , pt.intercompanysalesid
    , pt.payment
    , pt.projid
    , p.name as orderer
    , pt.partition
    , pt.[IsDelete]
    , pt.versionnumber
    , pt.sysrowversion

from {{ source('fno', 'purchtable') }} as pt

left join
    {{ source('fno', 'inventsite') }}
        as s
    on pt.inventsiteid = s.siteid
        and upper(s.dataareaid) = upper(pt.dataareaid)
left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as eps
    on lower(eps.[OptionSetName]) = lower('PurchStatus')
        and pt.purchstatus = eps.[Option]
left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as ept
    on lower(ept.[OptionSetName]) = lower('PurchaseType')
        and pt.purchasetype = ept.[Option]
left join
    {{ source('fno', 'inventlocation') }}
        as w
    on pt.inventlocationid = w.inventlocationid
        and upper(pt.dataareaid) = upper(w.dataareaid)

left join {{ source('fno', 'hcmworker') }} as h on pt.workerpurchplacer = h.recid
left join {{ ref('dim_d365_party') }} as p on h.person = p.party_recid
{%- if is_incremental() %}
    where pt.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pt.[IsDelete] is null
{% endif %}
