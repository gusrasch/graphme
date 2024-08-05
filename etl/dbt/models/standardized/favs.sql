{{ config(materialized = 'view') }}

WITH favs AS (
  SELECT
    SUM(length(favorited_by)) as num_favs,
    SUM(1) as num_messages,
    user_id,
    group_id,
  FROM 
      {{ ref('messages') }}
  GROUP BY user_id, group_id
)

SELECT
  mems.name,
  favs.num_favs,
  favs.num_messages,
  (favs.num_favs / favs.num_messages) AS avg_favs_per_mesage,
FROM
  favs
JOIN
  {{ ref('members') }} mems
ON
  (favs.user_id = mems.user_id) AND (favs.group_id = mems.group_id)
ORDER BY 
  avg_favs_per_mesage DESC
