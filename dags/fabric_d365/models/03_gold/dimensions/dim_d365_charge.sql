{{ config(
    materialized= 'incremental', 
    unique_key= ['dim_d365_charge_sk']
) }}

select
    m.[Id] as dim_d365_charge_sk
    , m.recid as charge_recid
    , m.markupcode as chargecode
    , m.maxamount
    , m.moduletype
    , m.taxitemgroup
    , m.txt as charge_desc
    , m.customerledgerdimension
    , m.custtype
    , m.custposting
    , e.[LocalizedLabel] as customer_posting
    , m.vendorledgerdimension

    , m.vendtype
    , m.vendposting
    , ev.[LocalizedLabel] as vendor_posting
    , m.partition
    , m.[IsDelete]
    , case
        when m.moduletype = 1 then 'Account Receivable'
        when m.moduletype = 2 then 'Account Payable'
        else ''
    end as chargetype
    , upper(m.dataareaid) as charge_dataareaid
from {{ source('fno', 'markuptable') }} as m
left join {{ source('fno', 'GlobalOptionsetMetadata') }} as e
    on
        m.custposting = e.[Option]
        and e.[OptionSetName] = 'custposting'
        and e.[EntityName] = 'markuptable'
left join {{ source('fno', 'GlobalOptionsetMetadata') }} as ev
    on
        m.vendposting = ev.[Option]
        and ev.[OptionSetName] = 'vendposting'
        and ev.[EntityName] = 'markuptable'
{%- if is_incremental() %}
where m.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
where m.[IsDelete] is null
{% endif -%}