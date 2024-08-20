SELECT
  mems.name AS author,
  mess.name AS alias,
  mess.text AS message,
  strftime(to_timestamp(mess.created_at), '%Y%m%d%H%M%S') AS created_at_ts,
FROM
  {{ ref('messages') }} mess
JOIN
  {{ ref('members') }} mems
ON
  (mess.user_id = mems.user_id) AND (mess.group_id = mems.group_id)
WHERE
    mess.text IS NOT NULL
ORDER BY 
    mess.created_at ASC
