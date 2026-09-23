begin;

drop view user_area_access;

CREATE OR REPLACE VIEW user_area_access AS
WITH access_join AS (
         SELECT u.id AS user_id,
            u.name AS user_name,
            ag.id AS ag_id,
            ag.name AS ag_name,
            ag.owner_id AS ag_owner_id,
            g.name AS group_name,
            uaga.access,
                CASE uaga.access
                    WHEN 'write'::access_level THEN 2
                    ELSE 1
                END AS priority
           FROM users u
             JOIN user_groups ug ON ug.user_id = u.id
             JOIN groups g ON g.name::text = ug.group_name::text
             JOIN user_area_groups_access uaga ON uaga.group_name::text = g.name::text
             JOIN area_groups ag ON ag.id = uaga.ag_id
        )
 SELECT access_join.user_id,
    access_join.user_name,
    access_join.ag_id,
    access_join.ag_name,
    access_join.ag_owner_id,
    max(access_join.priority) AS max_priority,
        CASE max(access_join.priority)
            WHEN 2 THEN 'write'::text
            ELSE 'read'::text
        END::access_level AS effective_access
   FROM access_join
  GROUP BY access_join.user_id, access_join.user_name, access_join.ag_id, access_join.ag_name, access_join.ag_owner_id;

commit;