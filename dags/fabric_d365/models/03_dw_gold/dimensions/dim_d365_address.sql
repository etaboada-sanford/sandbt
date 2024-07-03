select
    a.recid as address_recid,
    a.address,
    a.city,
    a.cityrecid,
    upper(a.countryregionid) as countryregionid,
    a.county,
    a.district,
    a.districtname,
    a.isprivate as address_isprivate,
    a.latitude,
    a.longitude,
    a.state,
    a.street,
    a.validfrom,
    a.validto,
    case
        when a.validto >= GetDate()
            or a.validto is null then 1
        else 0
    end as isvalid,
    a.zipcode,
    a.postbox,
    a.dxc_exportdeliveryterms,
    a.dxc_deliveryzone,
    a.dxc_locationcode,
    a.partition,
    a.IsDelete
from {{ source('fno', 'logisticspostaladdress') }} a
where IsDelete is null