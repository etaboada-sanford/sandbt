with navision as (
    select
        999999 as site_recid
        , 'NAV999' as siteid
        , 'Navision not migrated' as site_name
        , 'SANF' as site_dataareaid
        , null as timezone
        , null as defaultdimension
        , null as defaultinventstatusid
        , null as dxc_defaultdimension
        , null as partition
        , 0 as [IsDelete]
)

select
    st.[Id] as dim_d365_site_sk
    , st.recid as site_recid
    , st.siteid
    , st.name as site_name
    , upper(st.dataareaid) as site_dataareaid
    , st.timezone
    , st.defaultdimension
    , st.defaultinventstatusid
    , st.dxc_defaultdimension
    , st.partition
    , st.[IsDelete]
from {{ source('fno', 'inventsite') }} as st
where st.[IsDelete] is null
union all
/* Site not migrated from Navision */
select
    {{ dbt_utils.generate_surrogate_key(['site_recid', 'siteid']) }} as dim_d365_site_sk
    , *
from navision
