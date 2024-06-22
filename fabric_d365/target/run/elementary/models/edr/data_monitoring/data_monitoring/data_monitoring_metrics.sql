
        
            delete from "dw_gold"."dbo"."data_monitoring_metrics"
            where (
                id) in (
                select (id)
                from "dw_gold"."dbo"."data_monitoring_metrics__dbt_tmp"
            );
        
    

    insert into "dw_gold"."dbo"."data_monitoring_metrics" ("id", "full_table_name", "column_name", "metric_name", "metric_value", "source_value", "bucket_start", "bucket_end", "bucket_duration_hours", "updated_at", "dimension", "dimension_value", "metric_properties", "created_at")
    (
        select "id", "full_table_name", "column_name", "metric_name", "metric_value", "source_value", "bucket_start", "bucket_end", "bucket_duration_hours", "updated_at", "dimension", "dimension_value", "metric_properties", "created_at"
        from "dw_gold"."dbo"."data_monitoring_metrics__dbt_tmp"
    )
