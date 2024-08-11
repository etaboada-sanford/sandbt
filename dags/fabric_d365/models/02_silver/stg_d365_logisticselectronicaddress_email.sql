select
    a.recid
    , a.locator as primary_email
    , a.description as primary_email_desc
    , a.electronicaddressroles as primary_email_roles
from {{ source('fno', 'logisticselectronicaddress') }} as a
inner join {{ source('fno', 'dirpartytable') }} as b on a.recid = b.primarycontactemail
where a.locator is not null
