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
  ultimas_alturas_cume_dist.* 
FROM ultimas_alturas_cume_dist 
JOIN user_access
  ON user_access.red_id = ultimas_alturas_cume_dist.redid
WHERE
      redid= CASE WHEN %redid%!=0 THEN %redid% ELSE redid END
  AND tipo= CASE WHEN '%tipo%' != 'X' THEN '%tipo%' ELSE tipo END
