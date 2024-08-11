select
    v.[Id] as dim_d365_vendorpackingslip_sk

    , v.recid as vendorpackingslip_recid
    , v.orderaccount

    , v.invoiceaccount
    , v.packingslipid
    , v.defaultdimension

    , v.deliveryname
    , v.deliverypostaladdress
    , v.dlvmode
    , v.dlvterm

    , v.intercompanysalesid
    , v.purchid
    , v.purchasetype as purchasetypeid

    , ept.[LocalizedLabel] as purchasetype
    , upper(v.dataareaid) as vendorpackingslip_dataareaid

    , convert(date, v.deliverydate) as deliverydate
    , case when convert(date, v.documentdate) = '1900-01-01' then null else convert(date, v.documentdate) end as documentdate
    , upper(v.intercompanycompanyid) as intercompanycompanyid

    , coalesce(p.name, po.orderer) as requester


from {{ source('fno', 'vendpackingslipjour') }} as v
left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as ept
    on lower(ept.[OptionSetName]) = lower('PurchaseType')
        and v.purchasetype = ept.[Option]
left join {{ source('fno', 'hcmworker') }} as w on v.requester = w.recid
left join {{ ref('dim_d365_party') }} as p on w.person = p.party_recid

left join
    {{ ref('dim_d365_purchaseorder') }}
        as po
    on upper(v.dataareaid) = po.purchaseorder_dataareaid
        and v.purchid = po.purchid

where v.[IsDelete] is null
