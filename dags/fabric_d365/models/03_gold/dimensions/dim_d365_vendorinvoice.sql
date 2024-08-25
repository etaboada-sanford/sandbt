{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_vendorinvoice_sk']
) }}

select
    j.[Id] as dim_d365_vendorinvoice_sk

    , j.recid as vendorinvoice_recid
    , j.internalinvoiceid
    , j.invoiceid
    , j.orderaccount

    , j.invoiceaccount
    , j.currencycode

    , j.invoiceamount
    , j.invoiceamountmst
    , j.invoiceroundoff
    , j.defaultdimension
    , j.deliveryname

    , j.deliverypostaladdress
    , j.remittanceaddress
    , j.description
    , j.dlvmode
    , j.dlvterm

    , j.documentnum
    , j.exchrate
    , j.reportingcurrencyexchangerate
    , j.intercompanysalesid
    , j.payment
    , j.postingprofile
    , j.purchid
    , j.purchasetype as purchasetypeid

    , ept.[LocalizedLabel] as purchasetype
    , j.qty
    , j.salesbalance
    , upper(j.dataareaid) as vendorinvoice_dataareaid

    , convert(date, j.invoicedate) as invoicedate
    , convert(date, j.deliverydate_es) as deliverydate_es
    , convert(date, j.documentdate) as documentdate
    , convert(date, j.duedate) as duedate
    , convert(date, j.fixedduedate) as fixedduedate
    , case when convert(date, j.receiveddate) = '1900-01-01' then null else convert(date, j.receiveddate) end as receiveddate
    , upper(j.intercompanycompanyid) as intercompanycompanyid
    , j.versionnumber
    , j.sysrowversion
from {{ source('fno', 'vendinvoicejour') }} as j
left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as ept
    on lower(ept.[OptionSetName]) = lower('purchasetype')
        and j.purchasetype = ept.[Option]
{%- if is_incremental() %}
    where j.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where  j.[IsDelete] is null
{% endif %}
