{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_projgroup_sk']
) }}

select
    pr.[Id] as dim_d365_projgroup_sk
    , pr.recid
    , pr.projgroupid
    , pr.name
    , eprt.[LocalizedLabel] as projgrouptype
    , pr.costtranscost
    , pr.empltranscost
    , pr.invoiceposting
    , pr.itemtranscost
    , pr.ledgerposting
    , pr.projlinepropertysearch
    , pr.projtype
    , pr.partition
    , pr.recversion
    , pr.dxc_analysisfielddisplaycontrolid
    , pr.[IsDelete]
    , concat(pr.projgroupid, ' - ', pr.name) as projgroup_desc
    , upper(pr.dataareaid) as projgroup_dataareaid
    , pr.versionnumber
    , pr.sysrowversion

from {{ source('fno', 'projgroup') }} as pr
left join {{ source('fno', 'GlobalOptionsetMetadata') }} as eprt on eprt.[OptionSetName] = 'projtype'
    and pr.projtype = eprt.[Option]
    and eprt.[EntityName] = 'projgroup'
{%- if is_incremental() %}
    where pr.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pr.[IsDelete] is null
{% endif %}