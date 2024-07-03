SELECT
    recid inventdim_recid,
    inventdimid,
    inventbatchid,
    inventlocationid,
    wmslocationid,
    inventserialid,
    inventsiteid,
    inventstatusid,
    inventversionid,
    inventstyleid,
    licenseplateid,
    wmspalletid,
    UPPER(dataareaid) inventdim_dataareaid,
    PartitionId,
    createddatetime,
    IsDelete
FROM
    {{ source('fno', 'inventdim') }}
WHERE
    IsDelete is null
    