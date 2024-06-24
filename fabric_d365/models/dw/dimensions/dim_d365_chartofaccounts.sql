select
    ac.recid as chartofaccount_recid,
    ac.mainaccountid as chartofaccount,
    ac.name as chartofaccount_name,
    ac.accountcategoryref,
    acc.accountcategory,
    acc.accounttype,
    ac.IsDelete
from
    {{ source('dbo', 'mainaccount') }}
        as ac
    left join
        {{ source('dbo', 'mainaccountcategory') }}
            as acc
        on ac.accountcategoryref = acc.accountcategoryref
where
    ac.IsDelete is null