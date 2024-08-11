select *
from {{ ref('stg_d365_logisticselectronicaddress_contact') }}
where upper(role) like '%RFQ%'
