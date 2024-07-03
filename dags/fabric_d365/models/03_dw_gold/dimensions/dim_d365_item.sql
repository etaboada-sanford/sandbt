SELECT
    {{ dbt_utils.generate_surrogate_key(['i.recid']) }} AS dim_d365_item_sk,
    i.recid AS item_recid,
    UPPER(
        i.dataareaid
    ) AS item_dataareaid,
    i.itemid,
    i.namealias,
    pt.name AS item_name,
    i.itemtype,
    igi.itemgroupid,
    ig.name AS itemgroup_name,
    sdg.name AS itemstoragedimensiongroupid,
    sdg.description AS itemstoragedimensiongroup_name,
    i.batchnumgroupid,
    i.bomcalcgroupid,
    i.commissiongroupid,
    i.costgroupid,
    i.itembuyergroupid,
    i.itempricetolerancegroupid,
    i.packaginggroupid,
    i.pdsfreightallocationgroupid,
    i.pdsitemrebategroupid,
    i.prodgroupid,
    i.reqgroupid AS itemcoveragegroupid,
    i.serialnumgroupid,
    i.itmarrivalgroupid,
    i.itmoverundertolerancegroupid,
    i.itmcosttypegroupid,
    i.itmcosttransfergroupid,
    i.bomlevel,
    i.bomunitid,
    i.defaultdimension,
    i.netweight,
    i.productlifecyclestateid,
    i.salesmodel,
    i.dxc_speciescode speciescode,
    i.dxc_mpistate mpistate,
    i.dxc_mpistatecode mpistatecode,
    i.dxc_mpiunit mpiunit,
    i.dxc_appliedweight appliedweight,
    i.dxc_maximumweight maximumweight,
    i.dxc_storagecondition storagecondition,
    i.dxc_legacyitemcode legacyitemcode,
    i.dxc_labelformat labelformat,
    i.dxc_sizename sizename,
    i.dxc_licence licence,
    i.dxc_packagingcode packagingcode,
    i.dxc_primarypackagingtype primarypackagingtype,
    i.dxc_masterpackagingtype masterpackagingtype,
    i.dxc_processstatecode processstatecode,
    i.product,
    enum.producttype,
    CASE
        WHEN invenbl.itemid IS NOT NULL THEN 1
        ELSE NULL
    END AS whsinventenabled,
    CASE
        WHEN LOWER(
            i.namealias
        ) LIKE 'fish bin%'
        AND UPPER(
            igi.itemgroupid
        ) = 'SUNDRY' THEN 1
        ELSE 0
    END is_bin,
    i.partition,
    i.recversion,
    i.lastprocessedchange_datetime,
    ic.category_l_1 planning_category_l_1,
    ic.category_l_2 planning_category_l_2,
    ic.category_l_3 planning_category_l_3,
    ic.category_l_4 planning_category_l_4,
    ic.category_l_5 planning_category_l_5,
    ic.category_l_6 planning_category_l_6,
    ic.category_l_7 planning_category_l_7,
    ic.category_path planning_category_path,
    pck.shortname packagingshortname,CASE
        WHEN i.itemid LIKE '10%'
        OR i.itemid LIKE '20%'
        OR i.itemid LIKE '30%' THEN 1
        ELSE 0
    END AS seafooditem,
    i.IsDelete
FROM
    {{ ref('fno', 'inventtable') }}
    i
    LEFT JOIN {{ ref('fno', 'ecoresproduct') }}
    p
    ON i.product = p.recid
    AND p.IsDelete = false
    LEFT JOIN {{ ref('fno', 'ecoresproducttranslation') }}
    pt
    ON i.product = pt.recid
    AND pt.languageid = 'en-NZ'
    AND pt.IsDelete = false
    LEFT JOIN {{ ref('fno', 'inventitemgroupitem') }}
    igi
    ON i.itemid = igi.itemid
    AND UPPER(
        i.dataareaid
    ) = UPPER(
        igi.itemdataareaid
    )
    AND igi.IsDelete = false
    LEFT JOIN {{ ref('fno', 'inventitemgroup') }}
    ig
    ON igi.itemgroupid = ig.itemgroupid
    AND UPPER(
        i.dataareaid
    ) = UPPER(
        ig.dataareaid
    )
    AND ig.IsDelete = false
    LEFT JOIN {{ ref('fno', 'ecoresstoragedimensiongroupitem') }}
    sdig
    ON i.itemid = sdig.itemid
    AND UPPER(
        i.dataareaid
    ) = UPPER(
        sdig.itemdataareaid
    )
    AND sdig.IsDelete = false
    LEFT JOIN {{ ref('fno', 'ecoresstoragedimensiongroup') }}
    sdg
    ON sdig.storagedimensiongroup = sdg.recid
    AND sdg.IsDelete = false
    LEFT JOIN {{ ref('fno', 'whsinventenabled') }}
    invenbl
    ON i.itemid = invenbl.itemid
    AND UPPER(
        invenbl.dataareaid
    ) = UPPER(
        i.dataareaid
    )
    AND invenbl.IsDelete = false
    LEFT JOIN {{ ref('int_d365_dxc_enumstable_ecoresproducttype') }}
    enum
    ON p.producttype = enum.producttypeid
    LEFT JOIN {{ ref('int_dim_d365_itemcategory') }}
    ic
    ON i.recid = ic.item_recid
    AND UPPER(
        i.dataareaid
    ) = itemcategory_dataareaid
    AND ic.item_category = 'Planning'
    LEFT JOIN {{ ref('fno', 'dxc_packagingconfigurationtable') }}
    pck
    ON i.dxc_packagingcode = pck.packagingcode
    AND UPPER(
        i.dataareaid
    ) = UPPER(
        pck.dataareaid
    )
WHERE
    i.IsDelete is null
