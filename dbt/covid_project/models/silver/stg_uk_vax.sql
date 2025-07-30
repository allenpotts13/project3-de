{{config(materialized='incremental', unique_key='UID')}}

SELECT
    CONCAT(
        'Date', '_', 
        'AgeBand10Yr',
        'LGD2014Code'
    ) AS UID
FROM {{source('bronze','RAW_UK_VAX_COVERAGE')}}