SELECT
    *
FROM
read_json('../.local_data/json/*.json', 
    format = 'array',
    columns = {
        avatar_url: 'VARCHAR',
        created_at: 'BIGINT',
        favorited_by: 'VARCHAR[]',
        group_id: 'VARCHAR',
        id: 'VARCHAR',
        name: 'VARCHAR',
        sender_id: 'VARCHAR',
        sender_type: 'VARCHAR',
        source_guid: 'VARCHAR',
        system: 'BOOLEAN',
        text: 'VARCHAR',
        user_id: 'VARCHAR',
        platform: 'VARCHAR',
        pinned_by: 'VARCHAR',
        deleted_at: 'BIGINT',
        deletion_actor: 'VARCHAR'
    }
)
