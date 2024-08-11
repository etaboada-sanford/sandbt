with ctitemcat as (
    select
        x.*
        , rank() over (partition by x.item_cat_recid order by x.itemcategory_dataareaid) as rnkrec
    from (
        select
            pcat.recid as item_cat_recid
            , i.recid as item_recid
            , i.product
            , i.itemid
            , pt.name as item_name
            , enum.[LocalizedLabel] as producttype
            , i.partition
            , i.[Id]
            , i.[SinkModifiedOn]
            , cat.category_hierarchy as item_category
            , cat.category_path_level_1 as category_l_1
            , cat.category_path_level_2 as category_l_2
            , cat.category_path_level_3 as category_l_3
            , cat.category_path_level_4 as category_l_4
            , cat.category_path_level_5 as category_l_5
            , cat.category_path_level_6 as category_l_6
            , cat.category_path_level_7 as category_l_7
            , cat.category_path
            , pcat.category as category_recid
            , rank() over (partition by i.recid, cat.category_path_level_1 order by pcat.recid desc) as rnk
            , upper(i.dataareaid) as itemcategory_dataareaid
        from {{ source('fno', 'inventtable') }} as i
        inner join
            {{ source('fno', 'ecoresproduct') }}
                as p
            on i.product = p.recid
                and p.[IsDelete] is null
        left join
            {{ source('fno', 'ecoresproducttranslation') }}
                as pt
            on i.product = pt.recid
                and pt.[IsDelete] is null
                and pt.languageid = 'en-NZ'
        inner join
            {{ source('fno', 'ecoresproductcategory') }}
                as pcat
            on p.recid = pcat.product
                and pcat.[IsDelete] is null
        inner join {{ ref('dim_d365_category') }} as cat on pcat.category = cat.category_recid
        left join {{ source('fno', 'GlobalOptionsetMetadata') }} as enum
            on p.producttype = enum.[Option] and enum.[OptionSetName] = 'producttype'
        where i.[IsDelete] is null
    ) as x
    where x.rnk = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['cast(concat(cast(item_cat_recid as varchar),rnkrec) as bigint)']) }} as dim_d365_itemcategory_sk
    , convert(bigint, concat(convert(varchar, item_cat_recid), rnkrec)) as item_cat_recid_key
    , itemid
    , item_name
    , producttype
    , [SinkModifiedOn]
    , item_category
    , category_l_1
    , category_l_2
    , category_l_3
    , category_l_4
    , category_l_5
    , category_l_6
    , category_l_7
    , category_path
    , itemcategory_dataareaid
    , item_recid
    , category_recid
    , partition
from ctitemcat
