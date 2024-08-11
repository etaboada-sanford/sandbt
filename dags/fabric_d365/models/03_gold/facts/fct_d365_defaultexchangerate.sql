/* Default exchange rates to use in stage fact scripts */

select *
from (
    select 
        er.recid exchangerate_recid,
        pr.fromcurrencycode,
        pr.tocurrencycode,
        ert.name exchangerate_name,
        ert.description exchangerate_description,
        er.exchangerate*.01 exchangerate,
        convert(date, er.validfrom) validfrom,
        convert(date, er.validto) validto,
        er.partition

        , rank() over(partition by fromcurrencycode, tocurrencycode, validfrom, validto
                        order by er.createddatetime desc, er.recid desc) as rnk

    from {{sources('fno', 'exchangerate')}}  er 
    left join {{sources('fno', 'exchangeratecurrencypair')}} pr on er.exchangeratecurrencypair = pr.recid
    left join {{sources('fno', 'exchangeratetype')}} ert on pr.exchangeratetype = ert.recid
    where 
        er.IsDelete is null
        and pr.IsDelete is null
        and ert.IsDelete is null
        and exchangerate_name = 'Default'
)
where rnk = 1    