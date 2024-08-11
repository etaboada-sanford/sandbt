select
    pr.[Id] as dim_d365_project_sk
    , pr.recid as project_recid
    , pr.projid
    , pr.custaccount
    , pr.defaultdimension
    , fd.d1_businessunitvalue as businessunit
    , fd.d1_businessunit_name as businessunit_name
    , pr.deliverylocation
    , pr.dlvname
    , pr.jobid
    , pr.name
    , pr.parentid
    , pr.projgroupid
    , pg.projgroup_desc
    , pg.projgrouptype
    , pr.projinvoiceprojid
    , pr.projledgerposting
    , pr.projlinepropertysearch
    , pr.projpricegroup
    , pr.psaforecastmodelidexternal
    , pr.psaschedcalendarid
    , null as sortingid
    , null as sortingid2_
    , null as sortingid3_
    , e.[LocalizedLabel] as project_status
    , pr.taxgroupid
    , pr.type
    , pr.validateprojcategory
    , pt.name as project_responsible
    , pr.dxc_salmonunit as salmonunit
    , pr.dxc_salmonyearclass as salmonyearclass

    , pr.dxc_salmoninputid as salmoninputid
    , pr.dxc_salmonfishgroupnumber as salmonfishgroupnumber
    , pr.dxc_salmoncompanyid as salmoncompanyid
    , pr.dxc_salmonhatcherysupplierid as salmonhatcherysupplierid
    /*
    ,pr.workerresponsible
    ,pr.workerresponsiblefinancial
    ,pr.workerresponsiblesales
    */
    , pr.dxc_salmonsiteid as salmonsiteid

    , pr.dxc_salmonstageid as salmonstageid
    , pr.dxc_salmontypeid as salmontypeid
    , pr.dxc_musselfarm as musselfarm
    , pr.dxc_musselline as musselline
    , pr.dxc_musselcropid as musselcropid
    , pr.dxc_musselspattypeid as musselspattypeid
    , pr.dxc_musselcroptypeid as musselcroptypeid
    , pr.dxc_musselgrowingareaid as musselgrowingareaid
    , pr.dxc_musselsourcecropid1 as musselsourcecropid1
    , pr.dxc_musselmetreseeded1 as musselmetreseeded1
    , pr.dxc_musselsourcecropid2 as musselsourcecropid2
    , pr.dxc_musselmetreseeded2 as musselmetreseeded2
    , pr.partition
    , pr.[IsDelete]
    , case when convert(date, pr.created) = '1900-01-01' then null else convert(date, pr.created) end as created
    , concat(pr.projid, ' - ', pr.name) as proj_desc
    , case when convert(date, pr.startdate) = '1900-01-01' then null else convert(date, pr.startdate) end as startdate
    , case when convert(date, pr.projectedstartdate) = '1900-01-01' then null else convert(date, pr.projectedstartdate) end as projectedstartdate
    , case when convert(date, pr.projectedenddate) = '1900-01-01' then null else convert(date, pr.projectedenddate) end as projectedenddate
    , case when convert(date, pr.psaschedstartdate) = '1900-01-01' then null else convert(date, pr.psaschedstartdate) end as psaschedstartdate
    , case when convert(date, pr.psaschedenddate) = '1900-01-01' then null else convert(date, pr.psaschedenddate) end as psaschedenddate
    , convert(date, prcreateddatetime.newzealandtime) as createddatetime
    , convert(date, prmodifieddatetime.newzealandtime) as modifieddatetime
    , upper(pr.dataareaid) as project_dataareaid
from {{ source('fno', 'projtable') }} as pr
left join {{ source('fno', 'GlobalOptionsetMetadata') }} as e on e.[OptionSetName] = 'status' and pr.status = e.[Option] and e.[EntityName] = 'projtable'
left join {{ source('fno', 'hcmworker') }} as w on convert(varchar, pr.workerresponsible) = convert(varchar, w.recid) and w.[IsDelete] is null
left join {{ ref('dim_d365_party') }} as pt on w.person = pt.party_recid and pt.[IsDelete] is null
left join {{ ref('dim_d365_projgroup') }} as pg on pr.projgroupid = pg.projgroupid and upper(pr.dataareaid) = pg.projgroup_dataareaid and pg.[IsDelete] is null
left join {{ ref('dim_d365_financialdimensionvalueset') }} as fd on pr.defaultdimension = fd.financialdimensionvalueset_recid
cross apply dbo.ConvertUtcToNzt(pr.createddatetime) as prcreateddatetime
cross apply dbo.ConvertUtcToNzt(pr.modifieddatetime) as prmodifieddatetime
where pr.[IsDelete] is null
