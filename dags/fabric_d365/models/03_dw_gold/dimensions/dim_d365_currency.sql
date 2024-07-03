select
recid currency_recid,
currencycode,
symbol,
roundingprecision,
txt as currency_name,partition,
IsDelete
from {{ source('fno', 'currency') }}
where IsDelete is null