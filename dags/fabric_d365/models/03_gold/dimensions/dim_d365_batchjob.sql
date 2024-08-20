{{ config(
    materialized= 'incremental', 
    unique_key= ['dim_d365_batchjob_sk']
) }}

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
    , bj.versionnumber
    , bj.sysrowversion
from {{ source('fno', 'batchjob') }} as bj
{%- if is_incremental() %}
where bj.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
where abj.[IsDelete] is null
{% endif -%}
