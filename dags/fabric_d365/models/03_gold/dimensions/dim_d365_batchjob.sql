select
    bj.[Id] as dim_d365_batchjob_sk
    , bj.recid as batchjob_recid
    , bj.caption
    , bj.startdatetime
    , bj.enddatetime
    , bj.executingby
    , bj.[IsDelete]
    , upper(
        bj.company
    ) as batchjob_dataareaid
from {{ source('fno', 'batchjob') }} as bj
where bj.[IsDelete] is null
