{{config(materialized='incremental', unique_key='UID')}}

SELECT
    CONCAT(
        DGUID, '_', 
        VECTOR,
        COORDINATE
    ) AS UID
FROM {{source('bronze','RAW_CANADA_ANTIBODY_SEROPREVALENCE')}}