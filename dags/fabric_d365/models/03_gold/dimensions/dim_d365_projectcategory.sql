select 
    pc.[Id] as dim_d365_projectcategory_sk
    , pc.categoryid
    , pc.name as category_name
    , pc.categorygroupid
    , pc.active
    , upper(pc.dataareaid) projectcategory_dataareaid
    , pc.partition
    , pc.IsDelete

from {{source('fno', 'projcategory')}} pc
where pc.IsDelete is null