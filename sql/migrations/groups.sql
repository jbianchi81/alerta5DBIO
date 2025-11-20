BEGIN;

CREATE TYPE access_level AS ENUM ('none', 'read', 'write');

CREATE TABLE groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE user_groups (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, group_id)
);

CREATE TABLE red_group_access (
    red_id    INTEGER NOT NULL REFERENCES redes(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    access access_level NOT NULL DEFAULT 'none',
    PRIMARY KEY (group_id, red_id)
);

CREATE OR REPLACE VIEW user_red_access AS
WITH access_join AS (
    SELECT
        u.id      AS user_id,
        u.name    AS user_name,
        r.id      AS red_id,
        r.nombre  AS red_name,
        r.tabla_id,
        g.id      AS group_id,
        g.name    AS group_name,
        rga.access,
        CASE rga.access
            WHEN 'write' THEN 2
            WHEN 'read'  THEN 1
            ELSE 0
        END AS priority
    FROM users u
    JOIN user_groups ug      ON ug.user_id = u.id
    JOIN groups g            ON g.id = ug.group_id
    JOIN red_group_access rga ON rga.group_id = g.id
    JOIN redes r             ON r.id = rga.red_id
)
SELECT
    user_id,
    user_name,
    red_id,
    red_name,
    tabla_id,
    -- effective access is the MAX priority converted back to ENUM
    CASE MAX(priority)
        WHEN 2 THEN 'write'
        WHEN 1 THEN 'read'
        ELSE 'none'
    END::access_level AS effective_access
FROM access_join
GROUP BY user_id, user_name, red_id, red_name, tabla_id;

COMMIT;