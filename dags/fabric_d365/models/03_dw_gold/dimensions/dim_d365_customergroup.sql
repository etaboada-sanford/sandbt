select
    recid customergroup_recid,
    custgroup customergroup,
    defaultdimension,
    name customergroup_name,
    paymtermid,
    upper(dataareaid) customergroup_dataareaid,
    partition,
    IsDelete
from {{ source('fno', 'custgroup') }}
where IsDelete is null
