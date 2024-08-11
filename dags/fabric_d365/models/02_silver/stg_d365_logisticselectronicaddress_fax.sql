select
    a.recid
    , a.locator as primary_fax
    , a.description as primary_fax_desc
    , a.locatorextension as primary_fax_ext
    , a.electronicaddressroles as primary_fax_roles
from {{ source('fno', 'logisticselectronicaddress') }} as a
inner join {{ source('fno', 'dirpartytable') }} as b on a.recid = b.primarycontactfax
where a.locator is not null
