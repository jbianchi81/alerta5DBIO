BEGIN;

-- CREATE TYPE access_level AS ENUM ('read', 'write');

DROP VIEW user_area_access;
DROP TABLE user_area_groups_access;
DROP TABLE area_groups;

CREATE TABLE area_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    owner_id INTEGER NOT NULL REFERENCES users(id)
);

-- CREATE TABLE user_groups (
--     user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
--     group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
--     PRIMARY KEY (user_id, group_id)
-- );

-- ALTER TABLE areas_pluvio ADD COLUMN group_id INTEGER REFERENCES area_groups(id);

CREATE TABLE user_area_groups_access (
    ag_id    INTEGER NOT NULL REFERENCES area_groups(id) ON DELETE CASCADE,
    group_name VARCHAR NOT NULL REFERENCES groups(name) ON DELETE CASCADE,
    access access_level NOT NULL DEFAULT 'read',
    PRIMARY KEY (group_id, ag_id)
);

CREATE OR REPLACE VIEW user_area_access AS
WITH access_join AS (
    SELECT
        u.id      AS user_id,
        u.name    AS user_name,
        ag.id      AS ag_id,
        ag.name  AS ag_name,
        ag.owner_id AS ag_owner_id,
        g.name    AS group_name,
        uaga.access,
        CASE uaga.access
            WHEN 'write' THEN 2
            ELSE 1
        END AS priority
    FROM users u
    JOIN user_groups ug      ON ug.user_id = u.id
    JOIN groups g            ON g.name = ug.group_name
    JOIN user_area_groups_access uaga ON uaga.group_name = g.name
    JOIN area_groups ag             ON ag.id = uaga.ag_id
)
SELECT
    user_id,
    user_name,
    ag_id,
    ag_name,
    ag_owner_id,
    -- effective access is the MAX priority converted back to ENUM
    CASE MAX(priority)
        WHEN 2 THEN 'write'
        ELSE 'read'
    END::access_level AS effective_access
FROM access_join
GROUP BY user_id, user_name, ag_id, ag_name, ag_owner_id;

COMMIT;