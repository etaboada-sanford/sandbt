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

from {{ source('fno', 'projgroup') }} as pr
left join {{ source('fno', 'GlobalOptionsetMetadata') }} as eprt on eprt.[OptionSetName] = 'projtype'
    and pr.projtype = eprt.[Option]
    and eprt.[EntityName] = 'projgroup'
