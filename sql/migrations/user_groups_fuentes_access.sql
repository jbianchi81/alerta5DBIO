ALTER TABLE fuentes ADD COLUMN owner_id INTEGER REFERENCES users(id);

CREATE TABLE user_groups_fuentes_access (
    fuentes_id    INTEGER NOT NULL REFERENCES fuentes(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    access access_level NOT NULL DEFAULT 'read',
    PRIMARY KEY (group_id, fuentes_id)
);


CREATE OR REPLACE VIEW user_fuentes_access AS
WITH access_join AS (
    SELECT
        u.id      AS user_id,
        u.name    AS user_name,
        fuentes.id      AS fuentes_id,
        fuentes.nombre  AS fuentes_name,
        fuentes.owner_id AS fuentes_owner_id,
        g.id      AS group_id,
        g.name    AS group_name,
        ugfa.access,
        CASE ugfa.access
            WHEN 'write' THEN 2
            ELSE 1
        END AS priority
    FROM users u
    JOIN user_groups ug      ON ug.user_id = u.id
    JOIN groups g            ON g.id = ug.group_id
    JOIN user_groups_fuentes_access ugfa ON ugfa.group_id = g.id
    JOIN fuentes             ON fuentes.id = ugfa.fuentes_id
)
SELECT
    user_id,
    user_name,
    fuentes_id,
    fuentes_name,
    fuentes_owner_id,
    -- effective access is the MAX priority converted back to ENUM
    CASE MAX(priority)
        WHEN 2 THEN 'write'
        ELSE 'read'
    END::access_level AS effective_access
FROM access_join
GROUP BY user_id, user_name, fuentes_id, fuentes_name, fuentes_owner_id;
