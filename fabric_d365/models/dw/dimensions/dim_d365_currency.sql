select
recid currency_recid,
currencycode,
symbol,
roundingprecision,
txt as currency_name,partition,
IsDelete
from {{ source('dbo', 'currency') }}
where IsDelete is null