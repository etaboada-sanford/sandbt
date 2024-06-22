

-- depends_on: "dw_gold"."dbo"."elementary_test_results"
with empty_table as (
            select
            
                
        cast('this_is_just_a_long_dummy_string' as varchar(4096)) as elementary_test_results_id

,
                
        cast('this_is_just_a_long_dummy_string' as varchar(4096)) as result_row

,
                cast('2091-02-17' as DATETIME2(6)) as detected_at

,
                cast('2091-02-17' as DATETIME2(6)) as created_at


            )
        select * from empty_table
        where 1 = 0