{{ config(materialized = 'view') }}

SELECT
    group_id,
    UNNEST(members) as unnested,
FROM
    {{ source('groupme', 'groups') }}
