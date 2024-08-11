select
    v.[Id] as dim_d365_vendor_sk
    , v.recid as vendor_recid
    , v.accountnum
    , p.name
    , v.party as party_recid
    , v.vendgroup
    , p.orgnumber
    , v.dxc_vesselid as vesselid
    , v.dxc_vesselname as vesselname
    , v.youraccountnum
    , v.markupgroup
    , v.taxgroup
    , v.vatnum
    , v.paymtermid
    , v.paymmode
    , v.paymspec
    , v.currency
    , v.partition
    , v.blocked as blockedid
    , eb.[LocalizedLabel]
    , v.dxc_lfrnumber as lfrnumber
    , v.dxc_quotapermitcode as quotapermitcode
    , v.dxc_licencegroupid as licencegroupid
    , v.dxc_frozenpackaging as frozenpackaging
    , v.pricegroup
    , email_po.locators as email_po
    , email_invoice.locators as email_invoice
    , email_remit.locators as email_remitto
    , email_rfq.locators as email_rfq
    , ll.description as primary_address_description
    , lpa.street as primary_address_street
    , lpa.city as primary_address_city
    , lpa.state as primary_address_state
    , lpa.zipcode as primary_address_postcode
    , lpa.countryregionid as primary_address_countrycode
    , v.[IsDelete]
    , upper(
        v.dataareaid
    ) as vendor_dataareaid
    , nullif(v.blockedreleasedate, '1900-01-01') as blockedreleasedate
    , convert(date, v.createddatetime) as createddate
    , trim(
        case
            when right(
                    lpa.address
                    , 2
                ) = '%1' then left(lpa.address, len(lpa.address) - 2)
            else lpa.address
        end
    ) as primary_address
from
    {{ source('fno', 'vendtable') }} as v
left join {{ source('fno', 'vw_dirpartytable') }} as p on v.party = p.recid and p.[IsDelete] is null
left join {{ source('fno', 'logisticslocation') }} as ll on p.primaryaddresslocation = ll.recid
left join {{ source('fno', 'logisticspostaladdress') }} as lpa on ll.recid = lpa.location and getdate() between lpa.validfrom and lpa.validto
left join {{ source('fno', 'GlobalOptionsetMetadata') }} as eb on eb.[OptionSetName] = 'blocked' and eb.[EntityName] = 'vendtable' and v.blocked = eb.[Option]
left join {{ ref('stg_d365_logisticselectronicaddress_contact_poconfirm') }} as email_po on v.party = email_po.party_recid
left join {{ ref('stg_d365_logisticselectronicaddress_contact_invoice') }} as email_invoice on v.party = email_invoice.party_recid
left join {{ ref('stg_d365_logisticselectronicaddress_contact_remitto') }} as email_remit on v.party = email_remit.party_recid
left join {{ ref('stg_d365_logisticselectronicaddress_contact_rfq') }} as email_rfq on v.party = email_rfq.party_recid
where
    v.[IsDelete] is null
