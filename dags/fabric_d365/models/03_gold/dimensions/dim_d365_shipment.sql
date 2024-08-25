{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_shipment_sk']
) }}

with utcbasedata as (
    select
        wst.[Id] as dim_d365_shipment_sk
        , wst.recid as shipment_recid
        , wst.shipmentid
        , wst.shipmentstatus as shipmentstatusid
        , ess.[LocalizedLabel] as shipmentstatus
        , wst.accountnum
        , wst.address
        , ad.city
        , wst.carriercode
        , wst.carrierservicecode
        , wst.customerref
        , wst.deliveryname
        , wst.dlvtermid

        , wst.loadid
        , wst.ordernum
        , wst.modecode
        , eld.[LocalizedLabel] as loaddirection
        , wst.inventsiteid as siteid

        , s.name as site_name
        --, CONVERT_TIMEZONE('UTC', 'Pacific/Auckland', case when wst.dropoffutcdatetime = '1900-01-01 00:00:00.000' then null else wst.dropoffutcdatetime end) as dropoffdatetime_nzt
        , wst.inventlocationid as warehouseid
        --, CONVERT_TIMEZONE('UTC', 'Pacific/Auckland', case when wst.shipconfirmutcdatetime = '1900-01-01 00:00:00.000' then null else wst.shipconfirmutcdatetime end) as shipconfirmdatetime_nzt
        , w.name as warehouse_name
        --, CONVERT_TIMEZONE('UTC', 'Pacific/Auckland', case when wst.shipmentarrivalutcdatetime = '1900-01-01 00:00:00.000' then null else wst.shipmentarrivalutcdatetime end) as shipmentarrivaldatetime_nzt

        , wst.[IsDelete]
        , upper(ad.countryregionid) as countryregionid
        , case when wst.dropoffutcdatetime = '1900-01-01 00:00:00.000' then null else wst.dropoffutcdatetime end as dropoffdatetime
        , case when wst.shipconfirmutcdatetime = '1900-01-01 00:00:00.000' then null else wst.shipconfirmutcdatetime end as shipconfirmdatetime
        , case when wst.shipmentarrivalutcdatetime = '1900-01-01 00:00:00.000' then null else wst.shipmentarrivalutcdatetime end as shipmentarrivaldatetime
        , upper(wst.dataareaid) as shipment_dataareaid
        , wst.versionnumber
        , wst.sysrowversion

    from {{ source('fno', 'whsshipmenttable') }} as wst
    left join
        {{ source('fno', 'inventlocation') }}
            as w
        on wst.inventlocationid = w.inventlocationid
            and upper(wst.dataareaid) = upper(w.dataareaid)
    left join
        {{ source('fno', 'inventsite') }}
            as s
        on wst.inventsiteid = s.siteid
            and upper(wst.dataareaid) = upper(s.dataareaid)
    left join
        {{ source('fno', 'GlobalOptionsetMetadata') }}
            as ess
        on ess.[OptionSetName] = 'shipmentstatus'
            and wst.shipmentstatus = ess.[Option]
            and ess.[EntityName] = 'whsshipmenttable'
    left join
        {{ source('fno', 'GlobalOptionsetMetadata') }}
            as eld
        on eld.[OptionSetName] = 'loaddirection'
            and wst.loaddirection = eld.[Option]
            and eld.[EntityName] = 'whsshipmenttable'

    left join
        {{ source('fno', 'logisticspostaladdress') }}
            as ad
        on wst.deliverypostaladdress = ad.recid
            and ad.[IsDelete] is null

    {%- if is_incremental() %}
        where wst.sysrowversion > {{ get_max_sysrowversion() }}
    {%- else %}
        where wst.[IsDelete] is null
    {% endif %}
)

select
    ut.*
    , dropoff.newzealandtime as dropoffdatetime_nzt
    , shipconfirm.newzealandtime as shipconfirmdatetime_nzt
    , shipmentarrival.newzealandtime as shipmentarrivaldatetime_nzt
from utcbasedata as ut
cross apply dbo.f_convert_utc_to_nzt(ut.dropoffdatetime) as dropoff
cross apply dbo.f_convert_utc_to_nzt(ut.shipconfirmdatetime) as shipconfirm
cross apply dbo.f_convert_utc_to_nzt(ut.shipmentarrivaldatetime) as shipmentarrival
