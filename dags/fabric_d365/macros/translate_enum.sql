{% macro translate_enum(alias, field) -%}
JSON_VALUE({{ alias }}.enumtranslation, CONCAT('$.type."', {{ field }},'"'))
{%- endmacro %}