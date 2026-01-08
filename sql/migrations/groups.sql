BEGIN;

DROP VIEW IF EXISTS user_fuentes_access;
DROP TABLE IF EXISTS user_groups_fuentes_access;
DROP VIEW IF EXISTS user_area_access;
DROP TABLE IF EXISTS user_area_groups_access;
DROP TABLE IF EXISTS area_groups;

DROP VIEW IF EXISTS user_red_access;
DROP TABLE IF EXISTS red_group_access;
DROP TABLE IF EXISTS user_groups;
DROP TABLE IF EXISTS groups;

-- CREATE TYPE access_level AS ENUM ('read', 'write');

CREATE TABLE groups (
    name VARCHAR NOT NULL PRIMARY KEY
);

CREATE TABLE user_groups (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_name VARCHAR NOT NULL REFERENCES groups(name) ON DELETE CASCADE,
    PRIMARY KEY (user_id, group_name)
);

CREATE TABLE red_group_access (
    red_id    INTEGER NOT NULL REFERENCES redes(id) ON DELETE CASCADE,
    group_name VARCHAR NOT NULL REFERENCES groups(name) ON DELETE CASCADE,
    access access_level NOT NULL DEFAULT 'read',
    PRIMARY KEY (group_name, red_id)
);

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

CREATE TABLE area_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    owner_id INTEGER NOT NULL REFERENCES users(id)
);

ALTER TABLE areas_pluvio ADD COLUMN IF NOT EXISTS group_id INTEGER REFERENCES area_groups(id);

CREATE TABLE user_area_groups_access (
    ag_id    INTEGER NOT NULL REFERENCES area_groups(id) ON DELETE CASCADE,
    group_name VARCHAR NOT NULL REFERENCES groups(name) ON DELETE CASCADE,
    access access_level NOT NULL DEFAULT 'read',
    PRIMARY KEY (group_name, ag_id)
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
    MAX(priority) as max_priority,
    -- effective access is the MAX priority converted back to ENUM
    CASE MAX(priority)
        WHEN 2 THEN 'write'
        ELSE 'read'
    END::access_level AS effective_access
FROM access_join
GROUP BY user_id, user_name, ag_id, ag_name, ag_owner_id;

ALTER TABLE fuentes ADD COLUMN IF NOT EXISTS owner_id INTEGER REFERENCES users(id);

CREATE TABLE user_groups_fuentes_access (
    fuentes_id    INTEGER NOT NULL REFERENCES fuentes(id) ON DELETE CASCADE,
    group_name VARCHAR NOT NULL REFERENCES groups(name) ON DELETE CASCADE,
    access access_level NOT NULL DEFAULT 'read',
    PRIMARY KEY (group_name, fuentes_id)
);

CREATE VIEW user_fuentes_access AS
WITH access_join AS (
         SELECT u.id AS user_id,
            u.name AS user_name,
            fuentes.id AS fuentes_id,
            fuentes.nombre AS fuentes_name,
            fuentes.owner_id AS fuentes_owner_id,
            g.name AS group_name,
            ugfa.access,
                CASE ugfa.access
                    WHEN 'write'::access_level THEN 2
                    ELSE 1
                END AS priority
           FROM users u
             JOIN user_groups ug ON ug.user_id = u.id
             JOIN groups g ON g.name = ug.group_name
             JOIN user_groups_fuentes_access ugfa ON ugfa.group_name = g.name
             JOIN fuentes ON fuentes.id = ugfa.fuentes_id
        )
 SELECT access_join.user_id,
    access_join.user_name,
    access_join.fuentes_id,
    access_join.fuentes_name,
    access_join.fuentes_owner_id,
        CASE max(access_join.priority)
            WHEN 2 THEN 'write'::text
            ELSE 'read'::text
        END::access_level AS effective_access
   FROM access_join
  GROUP BY access_join.user_id, access_join.user_name, access_join.fuentes_id, access_join.fuentes_name, access_join.fuentes_owner_id;

COMMIT;