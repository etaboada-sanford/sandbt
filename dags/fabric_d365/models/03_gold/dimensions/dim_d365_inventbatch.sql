/* 
    createddatetime is in UTC and needs converting to NZT
    ods_d365_inventbatch
    ods_d365_pdshistoryinventdisposition 

    ctdt.newzealandtime as createddatetime_nzt

*/

with 
ct as (
    /* cr delim string of date, reason and details for dispostion*/
    select 
        upper(inventbatchid) as inventbatchid
        , itemid
        , replace(replace(
            STRING_AGG(concat(convert(date, ctdt.newzealandtime), '#', newdispositioncode, '#', dxc_inventdispositionreasoncode , '#' , dxc_batchdispositiondetails), '|')
            , '|', char(10)), '#', char(9)) as dxc_batchdispositiondetails_history
    from {{ source('fno', 'pdshistoryinventdisposition') }}
    cross apply dbo.ConvertUtcToNzt(createddatetime) as ctdt
    where coalesce(dxc_inventdispositionreasoncode, '') != ''
        or coalesce(dxc_batchdispositiondetails, '') != ''
    group by 
        upper(inventbatchid)
        , itemid
)

, ct1 as ( 
    /* most recent disposition record */
    select * from (
        select 
            upper(idh.inventbatchid) as inventbatchid
            , idh.itemid
            , idh.dxc_inventdispositionreasoncode inventdispositionreasoncode
            , idh.dxc_batchdispositiondetails batchdispositiondetails
            , rank() over (partition by upper(idh.inventbatchid), itemid order by ctdt.newzealandtime desc ) rnk
        from  {{ source('fno', 'pdshistoryinventdisposition') }} idh
        cross apply dbo.ConvertUtcToNzt(createddatetime) as ctdt
        where coalesce(dxc_inventdispositionreasoncode, '') != ''         
            or coalesce(dxc_batchdispositiondetails, '') != ''
    ) x 
    where rnk = 1
)

, cthist as (
    /* join history to latest disposition record */
    select 
        ct1.*
        , case when ct.dxc_batchdispositiondetails_history = '' then null else dxc_batchdispositiondetails_history end batchdispositiondetails_history
    from ct1 
    left join ct on ct1.inventbatchid = ct.inventbatchid and ct1.itemid = ct.itemid  
)

/* TBC - if batch created with QCHOLD and no history - only present for migrated records */
, ctqcflag as (
    select 
        upper(inventbatchid) as inventbatchid
        , itemid
        /* where batch put on QC hold today or yesterday, when today sat, sun, or mon include back to friday in y'day */
        , max(case when convert(date, ctdt.newzealandtime) = convert(date, getdate()) then 1 else 0 end) as qchold_today
        , max(case 
            when convert(date, ctdt.newzealandtime) = convert(date, dateadd(d, -1, getdate())) then 1 
            when dt_today.day_abbrev = 'Sun' 
                and convert(date, ctdt.newzealandtime) between convert(date, dateadd(d, -2, getdate())) and convert(date, dateadd(d, -1, getdate())) then 1 
            when dt_today.day_abbrev = 'Mon' 
                and convert(date, ctdt.newzealandtime) between convert(date, dateadd(d, -3, getdate())) and convert(date, dateadd(d, -1, getdate())) then 1
            else 0 end) as qchold_yesterday

        , max(convert(date, ctdt.newzealandtime)) as qchold_date

    from {{ source('fno', 'pdshistoryinventdisposition') }} pds
    join {{ ref('dim_date') }} dt_today on convert(date, getdate()) = dt_today.calendar_date    
    cross apply dbo.ConvertUtcToNzt(createddatetime) as ctdt
    where 
        IsDelete is null and 
        (coalesce(dxc_inventdispositionreasoncode, '') != ''
            or coalesce(dxc_batchdispositiondetails, '') != ''
        )
        and newdispositioncode = 'QCHOLD'
        
    group by 
        upper(inventbatchid)
        , itemid
)

, ctib as ( 
    select ib.[Id] as dim_d365_inventbatch_sk,
    ib.recid inventbatch_recid,
    upper(ib.inventbatchid) as inventbatchid,
    ib.itemid,
    convert(date, ib.proddate) proddate ,
    convert(date, ib.pdsbestbeforedate) pdsbestbeforedate ,
    convert(date, ib.pdsfinishedgoodsdatetested) pdsfinishedgoodsdatetested,
    convert(date, ib.pdsshelfadvicedate) pdsshelfadvicedate,
    convert(date, ib.pdsvendbatchdate) pdsvendbatchdate,
    ib.pdsdispositioncode,  /* current status (blank, available or .... )  */
    convert(date, ib.dxc_edecproductionstartdate) productionstartdate,
    /* change to same logic as used in the Transport Loadout Report */
    case when coalesce(convert(date, ib.dxc_edecproductionenddate), '1900-01-01') = '1900-01-01' then convert(date, ib.proddate) else convert(date, ib.dxc_edecproductionenddate) end productionenddate,

    upper(ib.dataareaid) inventbatch_dataareaid,
    ib.dxc_inventlocationid inventlocationid,
    ib.partition,
    ib.IsDelete,
    Case when coalesce(ib.pdsdispositioncode, '') in ('','Available') then null else cthist.inventdispositionreasoncode end inventdispositionreasoncode,
    Case when coalesce(ib.pdsdispositioncode, '') in ('','Available') then null else cthist.batchdispositiondetails end batchdispositiondetails,
    cthist.batchdispositiondetails_history
    
    /* change to same logic as used in the Transport Loadout Report */
    , case 
        when coalesce(convert(date, ib.proddate), '1900-01-01') not in ('1900-01-01', '2009-01-01')
            and it.pdsshelflife > 0
            and coalesce(convert(date, ib.expdate), '1900-01-01') = '1900-01-01'
            then convert(date, dateadd(day, it.pdsshelflife, ib.proddate))
            else convert(date, ib.expdate)
        end expdate

    /* dates for these are stored as integers past 1900-01-01
    Catch/HarvestDateEnd
    Catch/HarvestDateSta
    FreezingDate
    DateOfHarvest
    DateOfLanding
    */
    , case when coalesce(avce.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, cast(avce.pdsbatchattribvalue as int), '1900-01-01')) end as catchharvestdateend
    , case when coalesce(avcs.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, cast(avcs.pdsbatchattribvalue as int), '1900-01-01')) end as catchharvestdatestart
    , case when coalesce(avdh.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, cast(avdh.pdsbatchattribvalue as int), '1900-01-01')) end as dateofharvest
    , case when coalesce(avdl.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, cast(avdl.pdsbatchattribvalue as int), '1900-01-01')) end as dateoflanding
    , case when coalesce(avfd.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, cast(avfd.pdsbatchattribvalue as int), '1900-01-01')) end as freezingdate

    , avf.pdsbatchattribvalue as farmno
    , avca.pdsbatchattribvalue as catchharvestarea
    , avlp.pdsbatchattribvalue as landingport
    , avlc.pdsbatchattribvalue as linecageno
    , avm.pdsbatchattribvalue as manufacturerid 
    , avp.pdsbatchattribvalue as productionsite
    , upper(avt.pdsbatchattribvalue) as tripno
    , avvn.pdsbatchattribvalue as vesselname
    , avvnm.pdsbatchattribvalue as vesselnumber
    , coalesce(ctqcflag.qchold_today, 0)  as qchold_today
    , coalesce(ctqcflag.qchold_yesterday, 0)  as qchold_yesterday
    , ctqcflag.qchold_date

    from {{ source('fno', 'inventbatch') }} ib 
    left join cthist on upper(ib.inventbatchid) = cthist.inventbatchid 
        and ib.itemid = cthist.itemid  
    left join {{ source('fno', 'inventtable') }} it on ib.itemid = it.itemid  
        and upper(ib.dataareaid) = upper(it.dataareaid) 
        and it.IsDelete is null

    /* include all likely attributes used
    Catch/HarvestDateEnd
    Catch/HarvestDateSta
    CatchHarvestArea
    DateOfHarvest
    DateOfLanding
    FarmNo
    FreezingDate
    LandingPort
    LineCageNo
    ManufacturingId
    ProductionSite
    TripNo
    VesselName
    VesselNumber
    */

    left join {{ source('fno', 'pdsbatchattributes') }} avce on upper(ib.dataareaid) = upper(avce.dataareaid)
            and upper(avce.pdsbatchattribid) = upper('Catch/HarvestDateEnd')
            and upper(ib.inventbatchid) = upper(avce.inventbatchid)
            and ib.itemid = avce.itemid
            and avce.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avcs on upper(ib.dataareaid) = upper(avcs.dataareaid)
            and upper(avcs.pdsbatchattribid) = upper('Catch/HarvestDateSta')
            and upper(ib.inventbatchid) = upper(avcs.inventbatchid)
            and ib.itemid = avcs.itemid
            and avcs.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avca on upper(ib.dataareaid) = upper(avca.dataareaid)
            and upper(avca.pdsbatchattribid) = upper('CatchHarvestArea')
            and upper(ib.inventbatchid) = upper(avca.inventbatchid)
            and ib.itemid = avca.itemid
            and avca.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avdh on upper(ib.dataareaid) = upper(avdh.dataareaid)
            and upper(avdh.pdsbatchattribid) = upper('DateOfHarvest')
            and upper(ib.inventbatchid) = upper(avdh.inventbatchid)
            and ib.itemid = avdh.itemid
            and avdh.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avdl on upper(ib.dataareaid) = upper(avdl.dataareaid)
            and upper(avdl.pdsbatchattribid) = upper('DateOfLanding')
            and upper(ib.inventbatchid) = upper(avdl.inventbatchid)
            and ib.itemid = avdl.itemid
            and avdl.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avf on upper(ib.dataareaid) = upper(avf.dataareaid)
            and upper(avf.pdsbatchattribid) = upper('FarmNo')
            and upper(ib.inventbatchid) = upper(avf.inventbatchid)
            and ib.itemid = avf.itemid
            and avf.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avfd on upper(ib.dataareaid) = upper(avfd.dataareaid)
            and upper(avfd.pdsbatchattribid) = upper('FreezingDate')
            and upper(ib.inventbatchid) = upper(avfd.inventbatchid)
            and ib.itemid = avfd.itemid
            and avfd.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avlp on upper(ib.dataareaid) = upper(avlp.dataareaid)
            and upper(avlp.pdsbatchattribid) = upper('LandingPort')
            and upper(ib.inventbatchid) = upper(avlp.inventbatchid)
            and ib.itemid = avlp.itemid
            and avlp.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avlc on upper(ib.dataareaid) = upper(avlc.dataareaid)
            and upper(avlc.pdsbatchattribid) = upper('LineCageNo')
            and upper(ib.inventbatchid) = upper(avlc.inventbatchid)
            and ib.itemid = avlc.itemid
            and avlc.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avm on upper(ib.dataareaid) = upper(avm.dataareaid)
            and upper(avm.pdsbatchattribid) = upper('ManufacturingId')
            and upper(ib.inventbatchid) = upper(avm.inventbatchid)
            and ib.itemid = avm.itemid
            and avm.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avp on upper(ib.dataareaid) = upper(avp.dataareaid)
            and upper(avp.pdsbatchattribid) = upper('ProductionSite')
            and upper(ib.inventbatchid) = upper(avp.inventbatchid)
            and ib.itemid = avp.itemid
            and avp.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avt on upper(ib.dataareaid) = upper(avt.dataareaid)
            and upper(avt.pdsbatchattribid) = upper('TripNo')
            and upper(ib.inventbatchid) = upper(avt.inventbatchid)
            and ib.itemid = avt.itemid
            and avt.IsDelete is null
            
    left join {{ source('fno', 'pdsbatchattributes') }} avvn on upper(ib.dataareaid) = upper(avvn.dataareaid)
            and upper(avvn.pdsbatchattribid) = upper('VesselName')
            and upper(ib.inventbatchid) = upper(avvn.inventbatchid)
            and ib.itemid = avvn.itemid
            and avvn.IsDelete is null

    left join {{ source('fno', 'pdsbatchattributes') }} avvnm on upper(ib.dataareaid) = upper(avvnm.dataareaid)
            and upper(avvnm.pdsbatchattribid) = upper('VesselNumber')
            and upper(ib.inventbatchid) = upper(avvnm.inventbatchid)
            and ib.itemid = avvnm.itemid
            and avvnm.IsDelete is null

    left join ctqcflag on upper(ib.inventbatchid) = ctqcflag.inventbatchid 
        and ib.itemid = ctqcflag.itemid
            
    where ib.IsDelete is null
)

/* Approx 15K batch records do not have Trip details, but the InventBatchID is a TripID */
, ct_missing_trip as (
    select distinct
        ctib.dim_d365_inventbatch_sk
        , t.mserp_tripid as tripid
    from ctib
    /* must ref the ODS as stg_dim_d365_triporder refs stg_dim_d365_inventbatch */
    join {{ source('mserp', 'dxc_triporder') }} t on ctib.inventbatch_dataareaid = upper(t.mserp_dataareaid)
        and ctib.inventbatchid = t.mserp_tripid
    where ctib.tripno is null
)

, ctfinal as (
    select 
        ctib.dim_d365_inventbatch_sk,
        inventbatch_recid,
        inventbatchid,
        itemid,
        inventbatch_dataareaid,
        inventlocationid,
        partition,
        IsDelete,
        inventdispositionreasoncode,
        batchdispositiondetails,
        batchdispositiondetails_history,
        case when proddate = '1900-01-01' then null else proddate end proddate,
        case when pdsbestbeforedate = '1900-01-01' then null else pdsbestbeforedate end pdsbestbeforedate,
        case when pdsfinishedgoodsdatetested = '1900-01-01' then null else pdsfinishedgoodsdatetested end pdsfinishedgoodsdatetested,
        case when pdsshelfadvicedate = '1900-01-01' then null else pdsshelfadvicedate end pdsshelfadvicedate,
        case when pdsvendbatchdate = '1900-01-01' then null else pdsvendbatchdate end pdsvendbatchdate,
        case when pdsdispositioncode = '1900-01-01' then null else pdsdispositioncode end pdsdispositioncode,
        case when productionstartdate = '1900-01-01' then null else productionstartdate end productionstartdate,
        case when productionenddate = '1900-01-01' then null else productionenddate end productionenddate,
        case when expdate = '1900-01-01' then null else expdate end expdate

        , catchharvestdateend
        , catchharvestdatestart
        , catchharvestarea
        , dateofharvest
        , dateoflanding
        , farmno
        , freezingdate
        , landingport
        , linecageno
        , manufacturerid 
        , productionsite
        , coalesce(ctib.tripno, ct_missing_trip.tripid) as tripno
        , vesselname
        , vesselnumber
        , qchold_today
        , qchold_yesterday
        , qchold_date
    from ctib
    left join ct_missing_trip on ctib.dim_d365_inventbatch_sk = ct_missing_trip.dim_d365_inventbatch_sk
)

select * from ctfinal
