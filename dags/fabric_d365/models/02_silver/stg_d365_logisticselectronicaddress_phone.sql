select
    a.recid
    , a.locator as primary_ph
    , a.description as primary_ph_desc
    , a.locatorextension as primary_ph_ext
    , a.electronicaddressroles as primary_ph_roles
from {{ source('fno', 'logisticselectronicaddress') }} as a
inner join {{ source('fno', 'dirpartytable') }} as b on a.recid = b.primarycontactphone
where a.locator is not null
