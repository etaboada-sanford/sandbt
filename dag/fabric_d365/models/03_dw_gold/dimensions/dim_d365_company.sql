WITH ctco AS (
  SELECT
    ch.recid company_recid,
    chprty.recid party_recId,
    CONCAT(UPPER(ch.dataarea), ' - ', COALESCE(chprty.name, '')) company,
    COALESCE(chprty.name, '') company_name,
    UPPER(ch.dataarea) company_dataarea,
    COALESCE(pprty.name,'') parent_company_name,
    UPPER(parentc.dataarea) parent_co_dataarea,
    ch.coregnum,
    ch.bank,
    h.childorganization,
    h.parentorganization,
    h.hierarchytype,
    ht.name hierarchytypename,
    ch.IsDelete
  FROM {{ source('dbo', 'companyinfo') }}  ch
  LEFT JOIN  {{ source('dbo', 'dirpartytable') }} chprty ON ch.recid = chprty.recid
  LEFT JOIN {{ source('dbo', 'omhierarchyrelationship') }} h   ON h.childorganization = ch.recid AND h.hierarchytype = 5637145327 ---considering ONLY legal entities hierarchy
  LEFT JOIN {{ source('dbo', 'companyinfo') }}  parentc ON h.parentorganization = parentc.recid
  LEFT JOIN  {{ source('dbo', 'dirpartytable') }} pprty ON parentc.recid = pprty.recid
  LEFT JOIN {{ source('dbo', 'omhierarchytype') }} ht ON h.hierarchytype = ht.recid  ---considering ONLY legal entities hierarchy
 WHERE ch.IsDelete IS NULL

),
ctcopth AS (
SELECT 
company_recid,
party_recId,
parentorganization,
company_dataarea,
parent_co_dataarea,
company,
company_name,
parent_company_name,
sys_connect_by_path(company_name, ' -> ') company_path,
coregnum,
bank,
hierarchytypename
FROM ctco

start WITH parentorganization = 0
    CONNECT BY
      parentorganization = PRIOR childorganization
  ORDER BY childorganization
)
-----------------------------------------

SELECT 



*

 FROM (
SELECT

*
,
regexp_count(company_path, '->') company_path_level


FROM 
ctcopth
)x ORDER BY company_path_level


WITH tree (empid, name, level) AS  (
  SELECT empid, name, 1 as level
  FROM emp
  WHERE name = 'Joan'

  UNION ALL

  SELECT child.empid, child.name, parent.level + 1
  FROM emp as child
    JOIN tree parent on parent.empid = child.mgrid
)
SELECT name 
FROM tree;