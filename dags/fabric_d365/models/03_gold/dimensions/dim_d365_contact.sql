select
    lea.[Id] as dim_d365_contact_sk
    , lea.recid as contact_recid
    , lea.countryregioncode
    , lea.description
    , lea.location
    , lea.locator
    , lea.locatorextension
    , lea.electronicaddressroles
    , lea.isprimary
    , lea.ismobilephone
    , pl.party as party_recid
    , e.[LocalizedLabel] as logisticselectronicaddress_methodtype
    , lea.[IsDelete]

from {{ source('fno', 'logisticselectronicaddress') }} as lea
left join {{ source('fno', 'logisticslocation') }} as ll on lea.location = ll.recid
left join {{ source('fno', 'dirpartylocation') }} as pl on ll.recid = pl.location
left join {{ source('fno', 'GlobalOptionsetMetadata') }} as e
    on
        lea.type = e.[Option]
        and e.[OptionSetName] = 'type'
        and e.[EntityName] = 'logisticselectronicaddress'
where lea.[IsDelete] is null
