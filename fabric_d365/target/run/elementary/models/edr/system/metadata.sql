
  
    USE [dw_gold];
    
    

    EXEC('create view "dbo"."metadata__dbt_tmp_vw" as 

SELECT
    ''0.11.1'' as dbt_pkg_version;');




    
    
        EXEC('CREATE TABLE [dw_gold].[dbo].[metadata] AS (SELECT * FROM [dw_gold].[dbo].[metadata__dbt_tmp_vw]);');
    
    

  