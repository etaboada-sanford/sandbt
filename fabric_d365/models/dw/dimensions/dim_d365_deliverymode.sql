select 
recid deliverymode_recid,
code deliverymode,
txt deliverymode_desc,
upper(dataareaid) deliverymode_dataareaid,
 
partition,
IsDelete


from {{ source('dbo', 'dlvmode') }}
where IsDelete is null
