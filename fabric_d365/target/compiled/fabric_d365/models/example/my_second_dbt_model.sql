-- Use the `ref` function to select from other models

select *
from "dw_gold"."dbo"."my_first_dbt_model"
where id = 1