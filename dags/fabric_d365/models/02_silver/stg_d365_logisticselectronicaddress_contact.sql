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
        , {{ translate_enum('enum', 'lea.type' ) }}  as contacttype
        --, et.[LocalizedLabel] as contacttype
    from {{ source('fno', 'dirpartytable') }} as p
    inner join {{ source('fno', 'dirpartylocation') }} as pl
        on p.recid = pl.party
    inner join {{ source('fno', 'logisticslocation') }} as ll
        on pl.location = ll.recid
    inner join {{ source('fno', 'logisticselectronicaddress') }} as lea
        on ll.recid = lea.location
    cross apply stage.f_get_enum_translation('logisticselectronicaddress', '1033') enum
    -- inner join {{ source('fno', 'GlobalOptionsetMetadata') }} as et
    --    on lea.type = et.[Option] and et.[EntityName] = 'logisticselectronicaddress' and et.[OptionSetName] = 'type'
    -- where e t .[LocalizedLabel] = 'Email address'
    where {{ translate_enum('enum', 'lea.type' ) }} = 'Email address'
       and coalesce(lea.locator, '') != ''
)

select *
from electroniccontacts
