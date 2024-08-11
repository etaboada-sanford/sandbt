with electroniccontacts as (
    select
        p.recid as party_recid
        , lea.recid as contact_recid
        , lea.description as contact_description
        , lea.isprimary as contact_isprimary
        , lea.locator as locators
        , lea.locatorextension
        , lea.type
        , lea.electronicaddressroles as role
        , et.[LocalizedLabel] as contacttype
    from {{ source('fno', 'dirpartytable') }} as p
    inner join {{ source('fno', 'dirpartylocation') }} as pl
        on p.recid = pl.party
    inner join {{ source('fno', 'logisticslocation') }} as ll
        on pl.location = ll.recid
    inner join {{ source('fno', 'logisticselectronicaddress') }} as lea
        on ll.recid = lea.location
    inner join {{ source('fno', 'GlobalOptionsetMetadata') }} as et
        on lea.type = et.[Option] and et.[EntityName] = 'logisticselectronicaddress' and et.[OptionSetName] = 'type'
    where et.[LocalizedLabel] = 'Email address'
        and coalesce(lea.locator, '') != ''
)

select *
from electroniccontacts
