
  
    USE [dw_gold];
    
    

    EXEC('create view "dbo"."dbt_seeds__dbt_tmp_vw" as 

with empty_table as (
            select
            
                
        cast(''dummy_string'' as varchar(4096)) as unique_id

,
                
        cast(''dummy_string'' as varchar(4096)) as alias

,
                
        cast(''dummy_string'' as varchar(4096)) as checksum

,
                
        cast(''this_is_just_a_long_dummy_string'' as varchar(4096)) as tags

,
                
        cast(''this_is_just_a_long_dummy_string'' as varchar(4096)) as meta

,
                
        cast(''dummy_string'' as varchar(4096)) as owner

,
                
        cast(''dummy_string'' as varchar(4096)) as database_name

,
                
        cast(''dummy_string'' as varchar(4096)) as schema_name

,
                
        cast(''this_is_just_a_long_dummy_string'' as varchar(4096)) as description

,
                
        cast(''dummy_string'' as varchar(4096)) as name

,
                
        cast(''dummy_string'' as varchar(4096)) as package_name

,
                
        cast(''this_is_just_a_long_dummy_string'' as varchar(4096)) as original_path

,
                
        cast(''dummy_string'' as varchar(4096)) as path

,
                
        cast(''dummy_string'' as varchar(4096)) as generated_at

,
                
        cast(''dummy_string'' as varchar(4096)) as metadata_hash


            )
        select * from empty_table
        where 1 = 0;');




    
    
        EXEC('CREATE TABLE [dw_gold].[dbo].[dbt_seeds] AS (SELECT * FROM [dw_gold].[dbo].[dbt_seeds__dbt_tmp_vw]);');
    
    

  