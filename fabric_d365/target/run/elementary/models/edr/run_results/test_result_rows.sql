
        
            delete from "dw_gold"."dbo"."test_result_rows"
            where (
                elementary_test_results_id) in (
                select (elementary_test_results_id)
                from "dw_gold"."dbo"."test_result_rows__dbt_tmp"
            );
        
    

    insert into "dw_gold"."dbo"."test_result_rows" ("elementary_test_results_id", "result_row", "detected_at", "created_at")
    (
        select "elementary_test_results_id", "result_row", "detected_at", "created_at"
        from "dw_gold"."dbo"."test_result_rows__dbt_tmp"
    )
