select
    bj.recid as batchjob_recid,
    bj.caption,
    upper(
        bj.company
    ) as batchjob_dataareaid,
    bj.startdatetime,
    bj.enddatetime,
    bj.executingby,
    bj.IsDelete
from {{ source('fno', 'batchjob') }} as bj
where bj.IsDelete is null