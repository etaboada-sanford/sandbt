select
    p.[Id] as dim_d365_port_sk
    , p.recid as port_recid
    , p.portid
    , p.description as port_desc
    , p.dxc_country as country_code
    , c.country_isocode
    , c.country_name
    , c.continent
    , upper(p.dataareaid) as port_dataareaid

from {{ source('fno', 'intrastatport') }} as p
left join {{ ref('dim_d365_country') }} as c on p.dxc_country = c.countryregionid
where p.[IsDelete] is null
