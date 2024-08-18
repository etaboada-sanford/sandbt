with d365_sales as (
    select
        so.[Id] as dim_d365_salesorder_sk
        , so.recid as salesorder_recid
        , so.salesid
        , so.currencycode
        , so.custaccount
        , so.custgroup
        , so.customerref
        , so.deliveryname
        , so.deliverypostaladdress

        , so.dlvmode as deliverymode
        , d.txt as deliverymode_desc
        , so.dlvterm
        , so.email
        , so.defaultdimension
        , so.inventlocationid as warehouseid

        , l.name as warehouse_name
        , so.inventsiteid
        , si.name as site_name

        , so.invoiceaccount
        , so.payment
        , so.salesname

        , so.salespoolid
        , so.salesstatus as salesstatusid

        , ests.[LocalizedLabel]
            as salesstatus
        , so.salestype as salestypeid

        , est.[LocalizedLabel]
            as salestype
        , so.taxgroup

        , so.url
        , so.phone

        , so.dxc_placeofdelivery as placeofdelivery
        , so.dxc_deliveryzone
            as deliveryzone
        , e.[LocalizedLabel] as documentstatus
        , wst.worker_name as employee_sales_taker
        , wsr.worker_name
            as employee_sales_responsible
        , so.dxc_destinationlocationcode as destinationlocationcode
        , prt.port_desc
        , prt.country_code
        , prt.country_isocode
        , prt.country_name as port_country_name

        , prt.continent
        /*
                        convert(date, case when convert(date, so.createddatetime) = '1900-01-01' then null else so.createddatetime end) createdate,
                        */
        , so.partition

        -- 2023-07-12 table stg_dim_d365_salesorder_freightby_1 now has MODEs from LOADs and CARRIER SERVICE
        -- as well as the Sale Order & Route MODEs

        , so.[IsDelete]

        , upper(so.dataareaid) as salesorder_dataareaid

        , upper(
            l.dataareaid
        ) as warehouse_dataareaid
        , upper(
            si.dataareaid
        ) as site_dataareaid
        , convert(date, case when convert(date, so.deliverydate) = '1900-01-01' then null else so.deliverydate end) as deliverydate
        , convert(date, case when convert(date, so.dxc_orderpackdate) = '1900-01-01' then null else so.dxc_orderpackdate end) as orderpackdate

        /* reformatted for ease of reading...
                                case
                                when ctsu.rs_freight_by_ocean = 0 then
                                case
                                when ctsu.s_freight_by_ocean = 0 then
                                case
                                when so.dlvmode = 'TBC-EXPORT' then 1 else 0
                                end
                                else ctsu.s_freight_by_ocean
                                end
                                else ctsu.rs_freight_by_ocean
                                end sea_freight,

                                case
                                when ctsu.rs_freight_by_air = 0 then
                                case
                                when ctsu.s_freight_by_air = 0 then 0
                                else ctsu.s_freight_by_air
                                end
                                else ctsu.rs_freight_by_air
                                end air_freight ,

                                case
                                when ctsu.rs_freight_by_tl = 0 then
                                case
                                when ctsu.s_freight_by_tl = 0 then 0
                                else ctsu.s_freight_by_tl
                                end
                                else ctsu.rs_freight_by_tl
                                end local_freight,
                                */
        , convert(date, case when convert(date, so.dxc_requestedfinaldestinationdate) = '1900-01-01' then null else so.dxc_requestedfinaldestinationdate end) as requestedfinaldestinationdate
        , convert(date, case when convert(date, so.dxc_invoicedate) = '1900-01-01' then null else so.dxc_invoicedate end) as invoicedate
        , convert(date, case
            when convert(date, so.shippingdateconfirmed) != '1900-01-01' then convert(date, so.shippingdateconfirmed)
            when convert(date, so.shippingdaterequested) != '1900-01-01' then convert(date, so.shippingdaterequested)
        end) as shippingdate
        , convert(
            date, case when convert(date, so.dxc_requestedloadportdate) = '1900-01-01' then null else so.dxc_requestedloadportdate end
        ) as requestedloadportdate
        , convert(
            date, case when so.createddatetime = '1900-01-01 00:00:00.000' then null else createdate.newzealandtime end
        ) as createdate
        , case
            when ctsu.s_freight_by_ocean = 1 or ctsu.rs_freight_by_ocean = 1 or ctsu.l_freight_by_ocean = 1 or ctsu.cs_freight_by_ocean = 1 then 1
            when so.dlvmode = 'TBC-EXPORT' then 1
            else 0
        end
            as sea_freight

        , case
            when ctsu.s_freight_by_air = 1 or ctsu.rs_freight_by_air = 1 or ctsu.l_freight_by_air = 1 or ctsu.cs_freight_by_air = 1 then 1
            when so.dlvmode = 'TBC-AIR' then 1
            else 0
        end
            as air_freight
        , case
            when ctsu.s_freight_by_tl = 1 or ctsu.rs_freight_by_tl = 1 or ctsu.l_freight_by_tl = 1 or ctsu.cs_freight_by_tl = 1 then 1
            else 0
        /*case
                                                when so.dlvmode = 'TL' then 1 else 0
                                                end*/
        end as local_freight

    from {{ source('fno', 'salestable') }} as so

    left join {{ source('fno', 'inventlocation') }} as l on so.inventlocationid = l.inventlocationid and upper(so.dataareaid) = upper(l.dataareaid) and so.[IsDelete] is null
    left join {{ source('fno', 'inventsite') }} as si on so.inventsiteid = si.siteid and upper(si.dataareaid) = upper(l.dataareaid) and so.[IsDelete] is null

    left join {{ source('fno', 'GlobalOptionsetMetadata') }} as est on lower(est.[OptionSetName]) = 'salestype' and so.salestype = est.[Option] and est.[EntityName] = 'salestable' and so.[IsDelete] is null
    left join {{ source('fno', 'GlobalOptionsetMetadata') }} as ests on lower(ests.[OptionSetName]) = 'salesstatus' and so.salesstatus = ests.[Option] and ests.[EntityName] = 'salestable' and so.[IsDelete] is null

    left join {{ source('fno', 'dlvmode') }} as d on so.dlvmode = d.code and upper(so.dataareaid) = upper(d.dataareaid) and d.[IsDelete] is null

    left join {{ ref('stg_d365_salesorder_freightby_1') }} as ctsu on so.salesid = ctsu.salesid and upper(ctsu.dataareaid) = upper(so.dataareaid)

    left join {{ ref('dim_d365_worker') }} as wst on so.workersalestaker = wst.worker_recid
    left join {{ ref('dim_d365_worker') }} as wsr on so.workersalesresponsible = wsr.worker_recid
    left join {{ source('fno', 'GlobalOptionsetMetadata') }} as e on lower(e.[OptionSetName]) = 'documentstatus' and so.documentstatus = e.[Option] and est.[EntityName] = 'salestable'

    left join {{ ref('dim_d365_port') }} as prt on so.dxc_destinationlocationcode = prt.portid and prt.port_dataareaid = upper(so.dataareaid)
    cross apply dbo.f_convert_utc_to_nzt(so.createddatetime) as createdate
    where so.[IsDelete] is null

)

, lastld as (
    select x.*
    from (
        select
            dim_d365_load_sk
            , load_dataareaid
            , loadid
            , salesid
            , lastupdatedatetime_nzt
            , custvendref
            , eta_final_destination_date
            , estimated_departure_feeder_poa
            , estimated_departure_cedo
            , rank() over (partition by load_dataareaid, salesid order by lastupdatedatetime_nzt desc, dim_d365_load_sk desc) as rnk
        from {{ ref('dim_d365_load') }}
        where salesid is not null
    ) as x
    where x.rnk = 1
)

select
    d365_sales.dim_d365_salesorder_sk
    , d365_sales.salesorder_recid
    , d365_sales.salesorder_dataareaid
    , d365_sales.salesid
    , d365_sales.currencycode
    , d365_sales.custaccount
    , d365_sales.custgroup
    , d365_sales.customerref
    , d365_sales.deliveryname
    , d365_sales.deliverypostaladdress
    , d365_sales.deliverymode
    , d365_sales.deliverymode_desc
    , d365_sales.dlvterm
    , d365_sales.email
    , d365_sales.defaultdimension
    , d365_sales.warehouseid
    , d365_sales.warehouse_name
    , d365_sales.warehouse_dataareaid
    , d365_sales.inventsiteid
    , d365_sales.site_name
    , d365_sales.site_dataareaid
    , d365_sales.invoiceaccount
    , d365_sales.payment
    , d365_sales.salesname
    , d365_sales.salespoolid
    , d365_sales.salesstatusid
    , d365_sales.salesstatus
    , d365_sales.salestypeid
    , d365_sales.salestype
    , d365_sales.taxgroup
    , d365_sales.url
    , d365_sales.phone
    , d365_sales.placeofdelivery
    , d365_sales.deliverydate
    , d365_sales.orderpackdate
    , d365_sales.requestedfinaldestinationdate
    , d365_sales.invoicedate
    , d365_sales.shippingdate
    , d365_sales.requestedloadportdate
    , d365_sales.createdate
    , d365_sales.deliveryzone
    , d365_sales.sea_freight
    , d365_sales.air_freight
    , d365_sales.local_freight
    , d365_sales.employee_sales_taker
    , d365_sales.employee_sales_responsible
    , lastld.custvendref as latest_load_custvendref
    , lastld.eta_final_destination_date as latest_load_eta_final_destination_date
    , lastld.estimated_departure_feeder_poa as latest_load_estimated_departure_feeder_poa
    , lastld.estimated_departure_cedo as latest_load_estimated_departure_cedo
    , d365_sales.documentstatus
    , d365_sales.destinationlocationcode
    , d365_sales.port_desc
    , d365_sales.country_code
    , d365_sales.country_isocode
    , d365_sales.port_country_name
    , d365_sales.continent
    , d365_sales.partition
    , d365_sales.[IsDelete]

from d365_sales
left join lastld
    on d365_sales.salesorder_dataareaid = lastld.load_dataareaid
        and d365_sales.salesid = lastld.salesid

-- Add Dummy / NULL SalesOrder for historic NAV data

union all

select
    {{ dbt_utils.generate_surrogate_key(['-999999']) }} as dim_d365_salesorder_sk
    , -999999 as salesorder_recid
    , 'SANF' as salesorder_dataareaid
    , 'NAV00000' as salesid
    , null as currencycode
    , 'NAVUND00' as custaccount
    , 'LOCAL' as custgroup
    , null as customerref
    , null as deliveryname
    , null as deliverypostaladdress
    , null as deliverymode
    , null as deliverymode_desc
    , null as dlvterm
    , null as email
    , null as defaultdimension
    , null as warehouseid
    , null as warehouse_name
    , null as warehouse_dataareaid
    , null as inventsiteid
    , null as site_name
    , null as site_dataareaid
    , 'NAVUND00' as invoiceaccount
    , null as payment
    , null as salesname
    , null as salespoolid
    , 3 as salesstatusid
    , 'INVOICED' as salesstatus
    , 3 as salestypeid
    , 'Sales order' as salestype
    , null as taxgroup
    , null as url
    , null as phone
    , null as placeofdelivery
    , null as deliverydate
    , null as orderpackdate
    , null as requestedfinaldestinationdate
    , null as invoicedate
    , null as shippingdate
    , null as requestedloadportdate
    , null as createdate
    , null as deliveryzone
    , null as sea_freight
    , null as air_freight
    , null as local_freight
    , null as employee_sales_taker
    , null as employee_sales_responsible
    , null as latest_load_custvendref
    , null as latest_load_eta_final_destination_date
    , null as latest_load_estimated_departure_feeder_poa
    , null as latest_load_estimated_departure_cedo
    , 'Invoice' as documentstatus
    , null as destinationlocationcode
    , null as port_desc
    , null as country_code
    , null as country_isocode
    , null as port_country_name
    , null as continent
    , 5637144576 as partition
    , 0 as [IsDelete]
