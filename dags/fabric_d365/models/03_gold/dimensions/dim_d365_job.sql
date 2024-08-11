select
    j.[Id] as dim_d365_job_sk
    , j.recid as job_recid
    , j.jobid
    , j.partition
    , j.recversion
    , j.[IsDelete]
from {{ source('fno', 'hcmjob') }} as j
