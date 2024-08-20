{{ config(
    materialized= 'incremental', 
    unique_key= ['dim_d365_addresscontactrole_sk']
) }}

select
    [Id] as dim_d365_addresscontactrole_sk
    , recid as addresscontactrole_recid
    , iscontactinfo
    , ispostaladdress
    , name as addresscontactrole_name
    , type as addresscontactrole_type
    , partition
    , [IsDelete]
    , versionnumber
    , sysrowversion
from {{ source('fno', 'logisticslocationrole') }}
{%- if is_incremental() %}
where sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
where [IsDelete] is null
{% endif -%}