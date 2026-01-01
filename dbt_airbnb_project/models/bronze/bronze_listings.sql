{# {% set incremental_flag = 1 %}
{% set increamental_col = 'CREATED_AT' %}

select * from {{ source('staging', 'listings') }}

{% if incremental_flag == 1 %}
    where {{ increamental_col }} > (select COALESCE(MAX({{ increamental_col }}), '1900-01-01') FROM {{ this }} )
{% endif %} #}

{{
  config(
    materialized = 'incremental',
    )
}}

select * from {{ source('staging', 'listings') }}

{% if is_incremental() %}
    where CREATED_AT > (select COALESCE(MAX(CREATED_AT), '1900-01-01') FROM {{ this }} )
{% endif %}