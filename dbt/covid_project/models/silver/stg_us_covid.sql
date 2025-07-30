{{config(materialized='incremental', unique_key='COVID_RECORD_ID')}}

SELECT
    COVID_RECORD_ID
FROM {{source('bronze','US_COVID_FLATTENED_DATA')}}