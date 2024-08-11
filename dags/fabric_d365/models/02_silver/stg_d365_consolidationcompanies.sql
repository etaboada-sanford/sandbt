{{ config
(
    materialized = 'table', 
    transient = true,
    tags = ['refresh_once']
) }}



 select 'AFML' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'AFPL' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'ASFL' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'BRCO' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'BTNL' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'ELMA' company_dataarea,'ZSAA' consolidated_parent_co_dataarea Union all
select 'ELMG' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'MSDL' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'NIML' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'NZJT' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'OFFL' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'PERN' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'PHVL' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'PRMN' company_dataarea,'ZSAA' consolidated_parent_co_dataarea Union all
select 'PSHL' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'SANA' company_dataarea,'ZSAA' consolidated_parent_co_dataarea Union all
select 'SANF' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'SANI' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'SANW' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'SFML' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'SLTI' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'SPAT' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'SSAL' company_dataarea,'ZSAA' consolidated_parent_co_dataarea Union all
select 'TSGP' company_dataarea,'ZSAN' consolidated_parent_co_dataarea Union all
select 'ZSAA' company_dataarea,'' consolidated_parent_co_dataarea Union all
select 'ZSAN' company_dataarea,'' consolidated_parent_co_dataarea 


