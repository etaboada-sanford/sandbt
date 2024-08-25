{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_purchaserequisition_sk']
) }}

select
    pt.[Id] as dim_d365_purchaserequisition_sk
    , pt.recid as purchaserequisition_recid

    , pt.purchreqid
    , pt.purchreqname
    , sdat.newzealandtime as submitteddatetime_nzt
    , pt.createdby
    , pt.submittedby
    /* all valid dates have 37001 as timezone id = GMT */
    , p.name as originator

    , pt.purchreqtype as purchreqtypeid

    , eprt.[LocalizedLabel] as purchreqtype
    , pt.requisitionpurpose as requisitionpurposeid
    , eprp.[LocalizedLabel] as requisitionpurpose

    , pt.requisitionstatus as requisitionstatusid
    , eprs.[LocalizedLabel] as requisitionstatus
    , pt.projid
    , pt.partition
    , convert(date, pt.requireddate) as requesteddate
    , convert(date, pt.transdate) as accountingdate

    , case when convert(date, pt.submitteddatetime) = '1900-01-01' then null else pt.submitteddatetime end as submitteddatetime
    , upper(co.dataarea) as buyinglegalentity_dataareaid
    , pt.versionnumber
    , pt.sysrowversion

from {{ source('fno', 'purchreqtable') }} as pt
cross apply dbo.f_convert_utc_to_nzt(pt.submitteddatetime) as sdat

left join {{ source('fno', 'GlobalOptionsetMetadata') }} as eprt
    on pt.purchreqtype = eprt.[Option]
        and eprt.[OptionSetName] = 'purchreqtype'
        and eprt.[EntityName] = 'purchreqtable'

left join {{ source('fno', 'GlobalOptionsetMetadata') }} as eprp
    on pt.requisitionpurpose = eprp.[Option]
        and eprp.[OptionSetName] = 'requisitionpurpose'
        and eprp.[EntityName] = 'purchreqtable'

left join {{ source('fno', 'GlobalOptionsetMetadata') }} as eprs
    on pt.requisitionstatus = eprs.[Option]
        and eprs.[OptionSetName] = 'requisitionstatus'
        and eprs.[EntityName] = 'purchreqtable'

left join {{ source('fno', 'companyinfo') }} as co on pt.companyinfodefault = co.recid
left join {{ source('fno', 'hcmworker') }} as h on pt.originator = h.recid
left join {{ ref('dim_d365_party') }} as p on h.person = p.party_recid
{%- if is_incremental() %}
    where pt.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pt.[IsDelete] is null
{% endif %}
