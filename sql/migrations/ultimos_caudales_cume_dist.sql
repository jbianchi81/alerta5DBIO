WITH user_access AS (
  SELECT user_id,
    red_id
  FROM user_red_access
  WHERE user_red_access.user_id=COALESCE(
        token_user_id(
          '%token%', 
          current_setting('app.token_secret')
        ),
        10
      )
)
SELECT 
    ultimos_caudales_cume_dist.*
FROM ultimos_caudales_cume_dist
JOIN redes
ON ultimos_caudales_cume_dist.tabla = redes.tabla_id
JOIN user_access
 ON user_access.red_id = redes.id
