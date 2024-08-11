select
    ijt.[Id] as dim_d365_inventjournal_sk
    , ijt.recid as inventjournal_recid
    , ijt.journalid
    , ijn.journalnameid
    , ejt.[LocalizedLabel] as journaltype
    , ijt.description
    , ijt.posted
    /*
    , case
        when convert(date, ijt.posteddatetime) = '1900-01-01' then null
        when ijt.posteddatetimetzid = 37001 then ijtposteddatetime.NewZealandTime
        else ijt.posteddatetime
        end as posteddatetime
    */
    , ijtposteddatetime.newzealandtime as posteddatetime
    /* if user is removed from dirpersonuser the result will be null, so use the userid. Use D365BatchUser rather than username Enterprise */
    , ewfs.[LocalizedLabel] as workflowapprovalstatus
    , ijt.inventsiteid
    , ijt.inventlocationid
    , case when ijt.posteduserid = 'D365BatchUser' then 'D365BatchUser' else coalesce(p.name, ijt.posteduserid) end as postedby

    , upper(ijt.dataareaid) as inventjournal_dataareaid

from {{ source('fno', 'inventjournaltable') }} as ijt
inner join
    {{ source('fno', 'inventjournalname') }}
        as ijn
    on ijt.journalnameid = ijn.journalnameid
        and ijt.dataareaid = ijn.dataareaid

left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as ewfs
    on ewfs.[EntityName] = 'inventjournaltable'
        and ewfs.[OptionSetName] = 'workflowapprovalstatus'
        and ijt.workflowapprovalstatus = ewfs.[Option]

left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as ejt
    on ejt.[EntityName] = 'inventjournalname'
        and ewfs.[OptionSetName] = 'journaltype'
        and ijn.journaltype = ejt.[Option]

left join {{ source('fno', 'dirpersonuser') }} as u on ijt.posteduserid = u.[user]
left join {{ ref('dim_d365_party') }} as p on u.personparty = p.party_recid
cross apply dbo.ConvertUtcToNzt(ijt.posteddatetime) as ijtposteddatetime
where ijt.[IsDelete] is null
