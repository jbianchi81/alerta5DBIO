BEGIN;

CREATE OR REPLACE VIEW user_fuentes_access AS
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
          UNION ALL 
          SELECT 
            u.id AS user_id,
            u.name AS user_name,
            fuentes.id AS fuentes_id,
            fuentes.nombre AS fuentes_name,
            fuentes.owner_id AS fuentes_owner_id,
            null AS group_name,
            'write' AS access,
            2  AS priority
           FROM users AS u
           JOIN fuentes ON u.id = fuentes.owner_id
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