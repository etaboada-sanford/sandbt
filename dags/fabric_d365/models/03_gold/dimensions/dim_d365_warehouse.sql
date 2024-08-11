select
    il.[Id] as dim_d365_warehouse_sk
    , il.recid as warehouse_recid
    , il.inventlocationid as warehouseid
    , il.name as warehouse_name
    , upper(il.dataareaid) as warehouse_dataareaid
    , il.inventlocationidtransit
    , il.inventlocationlevel
    , il.inventlocationtype

    , st.recid as inventsite_recid
    , st.siteid
    , st.name as site_name
    , upper(st.dataareaid) as site_dataareaid
    , st.timezone
    , st.defaultdimension
    , st.defaultinventstatusid
    , st.dxc_defaultdimension
    , il.dxc_manufacturerid as manufacturerid
    , ewt.[LocalizedLabel] as warehousetype
    , eot.[LocalizedLabel]
        as operationstype

    , case
        when il.dxc_edecwarehousetype in (2, 3) then 1 else 0
    end as is_vessel /* ('EDECWHType_DW_VESSEL' ,'EDECWHType_LP_VESSEL')*/

    , case when (upper(il.name) like '%CREDIT%' or upper(il.name) like '%MANAGE%') then 1 else 0 end as is_credit /* ticket #58648 */

            /* Operations type is not N/A then Production warehouse
            then if 3PL/STORAGE or COOL warehouse is not Production
            Else is a Production warehouse
            */
    , case
        when il.dxc_operationstype != 0 then 1              /* 0 = Not applicable */
        when il.dxc_edecwarehousetype in (0, 1) then 0      /* 0 = 3PL/STORAGE, 1 = COOL */
        else 1
    end
        as is_production

    , il.partition
    , il.[IsDelete]

from {{ source('fno', 'inventlocation') }} as il
left join
    {{ source('fno', 'inventsite') }}
        as st
    on il.inventsiteid = st.siteid
        and upper(il.dataareaid) = upper(st.dataareaid)
        and st.[IsDelete] is null

left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as eot
    on eot.[OptionSetName] = 'dxc_operationstype'
        and il.dxc_operationstype = eot.[Option]

left join
    {{ source('fno', 'GlobalOptionsetMetadata') }}
        as ewt
    on ewt.[OptionSetName] = 'dxc_edecwarehousetype'
        and il.dxc_edecwarehousetype = ewt.[Option]

where il.[IsDelete] is null

union all
/* Null warehouse for un-mapped from Navision */
select
    {{ dbt_utils.generate_surrogate_key([999999, "\'NAV999\'"]) }} as dim_d365_warehouse_sk
    , 999999 as warehouse_recid
    , 'NAV999' as warehouseid
    , 'Navision not migrated' as warehouse_name
    , 'SANF' as warehouse_dataareaid
    , null as inventlocationidtransit
    , null as inventlocationlevel
    , null as inventlocationtype
    , 999999 as inventsite_recid
    , 'NAV999' as siteid
    , 'Navision not migrated' as site_name
    , 'SANF' as site_dataareaid
    , null as timezone
    , null as defaultdimension
    , null as defaultinventstatusid
    , null as dxc_defaultdimension
    , null as manufacturerid
    , null as warehousetype
    , null as operationstype
    , 0 as is_vessel /* ('EDECWHType_DW_VESSEL' ,'EDECWHType_LP_VESSEL')*/
    , 0 as is_credit /* ticket #58648 */
    , 0 as is_production
    , null as partition -- noqa: RF04
    , null as [IsDelete]
