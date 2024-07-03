{% if execute %}
    {%- set levels = dbt_utils.get_column_values(table = source('fno', 'ecorescategory'), column = 'level_', order_by = 'level_') -%}
{% else %}
    {%- set levels = [1, 2, 3] %}
{% endif %}

WITH ctcat AS (
    SELECT
        C.recid AS category_recid,
        C.parentcategory,
        C.level_ category_level,
        C.name AS category_name,
        C.categoryhierarchy,
        REPLACE(sys_connect_by_path(C.name, ' -> '), 'Product by Category and Group', 'Planning') category_path
    FROM
        {{ source('fno', 'ecorescategory') }} C
    WHERE
        C.is_deleted = FALSE
    START WITH
        parentcategory = 0
    CONNECT BY
        parentcategory = PRIOR recid
    ORDER BY
        category_level,
        recid
),
ctcat1 AS (
    SELECT
        C.*,
        pt.name parentcategory_name,
        cath.name category_hierarchy,
        ARRAY_TO_STRING(SPLIT(LTRIM(category_path, ' -> '), ' -> '), ',') category_path_level
    FROM
        ctcat C
        LEFT JOIN {{ source('fno', 'ecorescategory') }}
        pt
        ON C.parentcategory = pt.recid
        LEFT JOIN {{ soource('dbo', 'ecorescategoryhierarchy') }}
        cath
        ON C.categoryhierarchy = cath.recid
    UNION ALL
    SELECT
        {{ dbt_utils.generate_surrogate_key(['-999999']) }} AS dim_d365_category_sk,
        -999999 category_recid,
        -1 parentcategory,
        -1 category_level,
        '' category_name,
        -1 categoryhierarchy,
        '' category_path,
        '' parentcategory_name,
        '' category_hierarchy,
        '' category_path_level
)
SELECT
    * {% for level in levels -%},
        SPLIT_PART(
            category_path_level,
            ',',
            {{ level }}
        ) AS category_path_level_{{ level }}
    {% endfor -%},
    '' AS category_path_level_{{ levels|length + 1 }}
FROM
    ctcat1
