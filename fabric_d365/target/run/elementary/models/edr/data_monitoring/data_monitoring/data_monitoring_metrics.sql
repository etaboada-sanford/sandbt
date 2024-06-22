
  
    USE [dw_gold];
    
    

    EXEC('create view "dbo"."data_monitoring_metrics__dbt_tmp_vw" as 


    
    
        
    
    with empty_table as (
            select
            
                
        cast(''dummy_string'' as varchar(4096)) as id

,
                
        cast(''dummy_string'' as varchar(4096)) as full_table_name

,
                
        cast(''dummy_string'' as varchar(4096)) as column_name

,
                
        cast(''dummy_string'' as varchar(4096)) as metric_name

,
                
        cast(123456789.99 as FLOAT) as metric_value

,
                
        cast(''dummy_string'' as varchar(4096)) as source_value

,
                cast(''2091-02-17'' as DATETIME2(6)) as bucket_start

,
                cast(''2091-02-17'' as DATETIME2(6)) as bucket_end

,
                
        cast(123456789 as INT) as bucket_duration_hours

,
                cast(''2091-02-17'' as DATETIME2(6)) as updated_at

,
                
        cast(''dummy_string'' as varchar(4096)) as dimension

,
                
        cast(''dummy_string'' as varchar(4096)) as dimension_value

,
                
        cast(''dummy_string'' as varchar(4096)) as metric_properties

,
                cast(''2091-02-17'' as DATETIME2(6)) as created_at


            )
        select * from empty_table
        where 1 = 0
;');




    
    
        EXEC('CREATE TABLE [dw_gold].[dbo].[data_monitoring_metrics] AS (SELECT * FROM [dw_gold].[dbo].[data_monitoring_metrics__dbt_tmp_vw]);');
    
    

  