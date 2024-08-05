SELECT
    group_id,
    unnested.*,
FROM
    {{ ref('members_unnested') }}
