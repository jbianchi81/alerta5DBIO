begin;

drop view user_red_access;

CREATE OR REPLACE VIEW user_red_access AS
WITH access_join AS (
    SELECT
        u.id      AS user_id,
        u.name    AS user_name,
        r.id      AS red_id,
        r.nombre  AS red_name,
        r.tabla_id,
        g.name    AS group_name,
        rga.access,
        CASE rga.access
            WHEN 'write' THEN 2
            ELSE 1
        END AS priority
    FROM users u
    JOIN user_groups ug      ON ug.user_id = u.id
    JOIN groups g            ON g.name = ug.group_name
    JOIN red_group_access rga ON rga.group_name = g.name
    JOIN redes r             ON r.id = rga.red_id
)
SELECT
    user_id,
    user_name,
    red_id,
    red_name,
    tabla_id,
    -- effective access is the MAX priority converted back to ENUM
    MAX(priority) AS max_priority,
    CASE MAX(priority)
        WHEN 2 THEN 'write'
        ELSE 'read'
    END::access_level AS effective_access
FROM access_join
GROUP BY user_id, user_name, red_id, red_name, tabla_id;

commit;